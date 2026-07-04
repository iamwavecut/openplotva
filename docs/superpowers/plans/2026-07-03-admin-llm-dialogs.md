# Admin «LLM Dialogs» — план работ (редизайн раздела LLM Context)

Спека/хендоф с разведкой и обоснованиями:
`docs/superpowers/specs/2026-07-03-admin-llm-dialogs-design.md`.
Линии кода сверены 2026-07-03; перепроверять перед правкой.

## Context

Раздел админки «LLM Context» показывает плоский поток низкоуровневых LLM-запросов с
неудобным eager-JSON-деревом: метаданных в списке мало (запись не понять не открыв),
фрагментировано (один диалоговый оборот = несколько несвязанных записей), фильтра нет.
После выкатки session engine диалоговый оборот — это агентная сессия (N раундов
запрос-ответ + tool calls). Раздел перестраивается в **«LLM Dialogs»**: одна запись =
один агентный run со всеми раундами, обстоятельствами и местом вызова; one-off вызовы
(memory extraction, vision, shield…) — те же записи с одним раундом (fallback).

## Решения владельца (binding)

1. Группируем **все многошаговые потоки**: dialog-сессии, song/image оптимизаторы,
   console-сессии виртуального диалога; one-off — единый fallback-формат.
2. Хранение: **in-memory** кольцо run-записей («скелет»), рестарт = чистый лист;
   «мясо» (raw тела) добирается при просмотре из мест, где оно прихранено.
3. Старый плоский вид **заменяется полностью**; обновление — только ручной refresh.
4. Мини-кокпит KPI сверху — сразу. Деталь: верхнеуровневая визуализация + провал вниз
   по уровням (дизайн делегирован — см. §Frontend).
5. `run_id` добавить и в `llm_request_events` (SQL-аналитика группирует задним числом).
6. GraphQL `llmRequests` — операторский контракт, не трогаем. Admin REST
   `GET/POST /admin/api/llm/requests*` — единственный потребитель это сама вкладка
   (+ playwright smoke) — выводится из эксплуатации вместе с фронтом.

## Верифицированные факты (поправки к интуиции)

- Raw тела запросов/ответов **не персистятся никуда**: `llm_request_events` INSERT
  (runtime_llm.rs:32-71) без raw-колонок; тела живут только в кольце
  `RuntimeLlmTraceBuffer` (1000, без env-ручки, wiring lib.rs:~9800).
- **Song/image оптимизаторы сегодня вообще не трейсятся**: `build_request`
  (agent_runtime.rs) не ставит `request.trace`, а `AifarmHttpClient::emit_call_trace`
  ранне-выходит при None. Их надо начать трейсить (само по себе — observability-win).
- Прецедент сжатия: `139_whitecircle_checks_lz4` — Postgres column `SET COMPRESSION lz4`
  на большие JSONB; gzip на стороне приложения не нужен.
- Спавнов, рвущих task-local, на LLM-путях нет (WhiteCircle-аудит спавнится, но
  LlmCallRecord не производит); walker бежит инлайн — доказано существующим
  `TURN_DEADLINE` (budget.rs:15-20, session.rs:300-310).
- Vision/media материализуется **внутри** `execute_dialog_turn` (engine.rs:123-125) —
  скоуп вокруг всего turn ловит вложенные aux-вызовы.
- `DialogTurnObserver::record` зовётся ровно один раз на turn из finalize (engine.rs:602-612)
  — идеальная точка обогащения run-записи исходом без ломки single-exit.
- `DialogJobParams.message_text` (taskman lib.rs:352) — источник trigger-превью.
- Последняя миграция — 147 → новые 148/149.
- Фронт: вкладка `data-tab="llm"` (id не менять), label index.html:58, title map :1207,
  hook :1222; JS-блок :4404-5064 + :5271-5299; сплит-пейн + eager JSON-дерево;
  pre-existing баг: `toggleLLMDetails` вызывается, но не определён (кнопка Close кидает) —
  умирает вместе со старым кодом. Хеши ассетов: openplotva-web/src/lib.rs:35-75,
  guard-тесты :488-556.

## Backend

### Phase A — корреляция + миграция (поведение не меняется)

1. **`openplotva-llm/src/trace.rs`**: тип `LlmRunScope { run_id: String, run_kind: String }`,
   `tokio::task_local! LLM_RUN_SCOPE`, `with_run_scope(scope, fut)`, `current_run_scope()`.
   `LlmCallRecord` получает `run: Option<LlmRunScope>`; в
   `LlmCallTraceRegistry::observe(mut record)` — если `record.run.is_none()`, штампуем из
   task-local. Одна точка покрывает все emit-сайты (aifarm dialog+aux, gemini dialog+aux)
   без правки клиентов. Реэкспорт из lib.rs.
2. **Трейс оптимизаторов**: в `AifarmReasoner::complete` (agent_runtime.rs:242-294)
   проставить `request.trace` (source `aifarm_agent`, flow = workflow_key
   `agentic_song|agentic_image|…`, context из self.context) на обеих ветках
   (walker + direct).
3. **Миграция 148**: `llm_request_events` + `run_id TEXT`, `run_seq INTEGER`,
   partial index по run_id. Writer: `RuntimeLlmRequestData` (openplotva-server,
   runtime_graphql.rs:854-882) + `LlmRequestEvent` (runtime_llm.rs:838-878) получают
   run_id/run_seq; INSERT-префикс и binds расширить; GraphQL `LlmRequest` их не
   экспонирует (контракт байт-в-байт). `RuntimeLlmTraceBuffer::record` начинает
   **возвращать id** (нужен раундам).

### Phase B — run-записи

4. **Новый модуль `openplotva-app/src/runtime_llm_runs.rs`** — `RuntimeLlmRunBuffer`
   (open-map + closed-ring 512, Mutex без await, watchdog 30 мин → Abandoned):
   - `RunRecord { id, run_id, kind, origin, started_at, ended_at, status
     Running|Completed|Failed|Abandoned, rounds, totals, outcome }`;
   - `RunOrigin { chat_id, thread_id, chat_title?, user_id, user_full_name?,
     trigger_message_id, trigger_preview? (~120ch из DialogJobParams.message_text),
     queue_name?, job_id? }`;
   - `RunRound { seq, trace_id, at, provider, model, flow, is_aux, iteration,
     duration_ms, tokens in/out/total, error, response_text (cap 8000),
     sent: None|Intermediate|Final, tool_calls: Vec<RunToolCall{name,status,duration_ms}> }`;
   - API: `begin_run` (повторный begin того же id → старый в Abandoned), `record_round`
     (возвращает seq; бекфиллит chat_title), `record_tool_result` (к последнему раунду),
     `mark_round_sent(run_id, Intermediate|Final)`, `record_one_off` (unscoped трейс →
     закрытая одно-раундовая запись, kind = flow), `finish_run` (no-op для незнакомых id),
     `list(filter)` (open+ring, newest-first, **без** response_text, но с превью
     последнего ответа ~200ch), `get(id)`, `clear()`, `prune_chat(chat_id)`.
   Память: 512 × ~5 раундов × ≤8KB ≈ ≤20MB worst, типично 2-4MB.
5. **Fan-out в `RuntimeLlmObserver`** (runtime_llm.rs:473-498, +`runs` арг):
   trace в кольцо (id!) → RunRound из record → scoped → `record_round` (run_seq на DB-строку);
   unscoped → `record_one_off`. `is_aux` = flow ≠ kind-flow.
6. **Dialog-runs** (dialog_jobs/worker.rs:287-484): `DialogJobProcessOptions` +
   `llm_runs`; перед `execute_dialog_turn` — `begin_run("job-{item.id}", "dialog",
   origin из params)` и обёртка всего вызова в `with_run_scope` (ловит session, legacy
   и vision). Merged/parked/decode-пути run не открывают. **Закрытие через ledger-sink**:
   `DialogTurnObserver` + `runs`; в `record()` — `finish_run("job-{job_id}",
   статус из outcome, RunOutcome{outcome, reason, user_signal, sent_message_parts,
   side_effect_ticket_id, detail})`. retry_scheduled → Completed(retry), следующий
   attempt переоткрывает. **Тул-результаты и sent-маркеры**: `SessionRunContext` +
   `llm_runs`; рядом с `append_session_tool_event` → `record_tool_result`; в местах
   успешной отправки intermediate/final → `mark_round_sent`. Прокидку `llm_runs`
   через воркер-луп покрыть луп-левел тестом (урок PR #5: луп однажды молча
   ронял опцию session).
7. **Optimizer-runs** (agent_runtime.rs run_agent:~737 / refine_prompt:~992):
   `begin_run("song|image-{ms}-{n}", kind)`, `with_run_scope` вокруг цикла
   `advance_one_step`, `finish_run` по исходу; тул-обсервации state → `record_tool_result`.
8. **Console-runs** (runtime_virtual_dialog.rs:~240): `begin_run("console-{sid}-m{mid}",
   "console")` вокруг `run_captured_session`; cleanup-воркер (lib.rs:10262-10290)
   дополнительно зовёт `prune_chat`.
9. **Composition root** (lib.rs:9800-10021): создать буфер, пробросить в observer,
   turn-observer, worker options, agent-провайдеры, virtual executor, StaticWebRoutes.

### Phase C — «мясо» + admin API (+ GraphQL)

10. **Миграция 149**: `llm_request_events` + `raw_request JSONB`, `raw_response JSONB`
    (`SET COMPRESSION lz4`), partial index по created_at где raw не NULL.
    Writer: тела пишутся если ≤ `LLM_RAW_BODY_MAX_BYTES` (64KB) и включено;
    `PostgresRuntimeLlmEventRecorder::spawn(+RawBodyPolicy)`.
    **Scrub-воркер** (по образцу cleanup, runtime_llm.rs:717-762): раз в час NULL-ит raw
    старше `LLM_RAW_BODY_RETENTION_HOURS` (48ч), батчами 10k. Объём: ~0.3-0.6GB steady
    (lz4 ~4-8×), worst ≤5GB, самолечится и выключается ручками.
11. **Admin REST** (lib.rs: маршруты после :734 + `GO_ADMIN_API_ROUTE_PATTERNS` :797-841
    + parity-тест :15079):
    - `GET /admin/api/llm/dialogs` — скелеты: фильтры kind/flow/chat_id/errors_only/q/
      limit (default 200); `{"count": n, "runs": [...]}` без response_text/tool args,
      но с trigger/response превью, totals, outcome (фронт фильтрует клиентски, серверные
      параметры — на вырост);
    - `GET /admin/api/llm/dialogs/detail?id=` — полные раунды; raw для каждого:
      кольцо по trace_id (`raw_source:"live"`) → БД по `(run_id, run_seq)` → `"rotated_out"`;
    - `POST /admin/api/llm/dialogs/clear` — только run-буфер (кольцо трейсов живёт —
      его читает GraphQL llmRequests).
    Auth `require_admin_request`, ответы `admin_json_no_cache_response`/`{"error":…}`.
12. **GraphQL `llmRuns`** (аддитивно, скелеты без raw): trait `RuntimeLlmRunInspector` +
    типы + резолвер рядом с llm_requests; `llmRequests`/`dialogTurnOutcomes` нетронуты.
13. **Config** (openplotva-config, RuntimeApiConfig): `LLM_RUN_BUFFER_CAPACITY=512`,
    `LLM_RAW_BODY_PERSIST_ENABLED=true`, `LLM_RAW_BODY_MAX_BYTES=65536`,
    `LLM_RAW_BODY_RETENTION_HOURS=48`; `.env.example`.

## Frontend (Phase D — после C)

Файлы: `web/admin/index.html`, `admin.css`, `tokens.css`; `components.js/css` **не трогаем**
(ноль новых pl-компонентов — секция строится из существующих pl-* + CSS-классов `.llmd-*`,
прецедент Memory/Routing).

1. **Переименование**: index.html:58 label → «LLM Dialogs», :1207 title map, :1223 hook →
   `loadLLMDialogs(false)`. `data-tab="llm"`/`id="llm"` не менять.
2. **IA вкладки**: `#llmd-browse` (topbar «Agent runs · N loaded» + Refresh/Clear →
   KPI-строка → фильтр-карта → список) и `#llmd-detail` = **полноширинный `pl-drawer`,
   замещающий список** (Esc/«← Runs», восстановление скролла; сплит-пейн и резайзер
   умирают — они и делали старую деталь тесной).
3. **KPI (6 плиток `.metric-card`, по отфильтрованному набору)**: runs (of N loaded),
   errors % (danger-тинт), avg rounds (max), avg duration (p95), tokens (≈/run),
   tool calls (top tool). «По типам» живёт в фасет-чипах с каунтами (интерактивно).
4. **Фильтры**: ряд kind-чипов `dialog·song·image·console·memory·vision·shield·other`
   (single-select + All, каунты по классической фасет-семантике); provider/model
   pl-select (model сужается по provider), «Errors only» pl-toggle, поиск pl-input
   (debounce 200ms). Вся фильтрация клиентская по ~512 скелетам; состояние в JS,
   без URL-роутинга.
5. **Строка списка** (`.list-item`, левый борт 3px цвета типа, 4 строки):
   (1) время · kind-бейдж · outcome/status-бейдж (running — пульс) · provider@model ·
   справа duration + ×rounds; (2) чат/тред/юзер + trigger-превью; (3) чипы: тулы со
   статус-цветом (`search ×3`), tokens; (4) превью финального ответа, у failed — сниппет
   ошибки (danger) вместо превью.
6. **Деталь — прогрессивное раскрытие**:
   - **L0** мета-грид обстоятельств (чат/юзер/триггер, queue, длительность, totals,
     outcome+reason, sent parts, error полностью);
   - **L1 flow strip** — горизонтальная лента: пип на раунд (цвет модели из `--c-cat-N`
     stable-hash, номер внутри, danger-кольцо при ошибке, акцент на финальном),
     тул-глифы между, aux-раунды полразмера; клик — скролл к карточке;
   - **L2 лента раундов** — карточки-«беседа»: хедер (#N · provider@model · duration ·
     tokens · ✓/✕ · бейджи aux/final), тело: текст ответа как проза (clamp 8 строк),
     tool calls первым классом (имя-чип + args/result коллапсами, result — превью
     ~400ch + «Expand (12KB)» со скроллируемой панелью), `.llmd-sent` маркер
     «отправлено в чат» (флаг sent из бэка), intermediate/final выделены. По умолчанию
     раскрыты: последний не-aux раунд + все с ошибками (one-off — его единственный);
   - **L3 tech-панель** в раунде: usage/timings/TPS/inference params гридом + кнопки
     Raw request/response (ленивое JSON-дерево; `rotated_out` — честная заглушка) +
     Copy round JSON.
7. **JSON-дерево**: переписать на ленивое (`buildLazyJSONNode` — дети материализуются
   при первом раскрытии; лечит фриз на огромных payload). Сохраняем `.json-node*` CSS,
   `jsonNodeKind/Summary`, `isExpandableJSONString`, `bindJSONTree`; выпиливаем eager
   `buildJSONNode`, `LLM_TYPE_PALETTE` (rgba в JS!), весь старый JS-блок LLM Context.
8. **Токены** (tokens.css, после routing-палитр): `--c-flow-dialog|song|image|console|
   memory|vision|shield|other` — алиасы существующих примитивов; выбор через
   `.llmd-kind[data-kind]` → `--flow-c` (паттерн `.tag`).
9. **Состояния**: PL.skeleton/empty («ничего не поймано» / «фильтры пусты» + Reset) /
   PL.error+Retry для списка и детали; Clear через PL.confirm(danger); все fetch через
   `apiCall()`; `llmdCopyText` — общий clipboard-хелпер.
10. **Хеши**: sha256 для tokens.css/admin.css/index.html в openplotva-web/src/lib.rs;
    `cargo test -p openplotva-web`; смок `tools/service-smoke.web-ui.spec.js:387,445`
    (бьёт по старому эндпоинту) переписать на новый. Скилл
    `openplotva-design-system-review` перед мерджем. Старые REST
    `/admin/api/llm/requests*` и мёртвый JS удаляются в этой же фазе.

## Порядок работ и поставка

Четыре независимо-шипуемых слайса: **A** (корреляция+148, ничего не видно) → **B**
(run-буфер, виден через GraphQL llmRuns при желании) → **C** (149+scrub+REST) → **D**
(фронт-свап+переименование+retire старого). PR-флоу: ветка без само-атрибуции,
CI-бэбиситтинг с ежеминутным опросом, деплой workflow_dispatch, пост-деплой
проверка через Runtime API.

## Verification

- Юнит-матрица (бэкенд): скоуп-пропагация (внутри/вне, две параллельные сессии не
  бликуют, aux внутри dialog-run = is_aux), lifecycle буфера (begin/rounds/tools/sent/
  finish/one-off/re-begin/watchdog/evict/prune/clear), fan-out (trace_id==round.trace_id,
  run_seq в DB-строке), ledger-обогащение (finalize закрывает run; merged/parked не
  создают), трейс-тег оптимизаторов, writer SQL (колонки, cap→NULL, off→NULL), scrub SQL
  (только с телами, только старше cutoff, UPDATE не DELETE), REST (403/методы/фильтры/
  список без тел/деталь live|db|rotated_out/clear не трогает кольцо/parity-тест),
  GraphQL (llmRequests SDL заморожен; llmRuns скелеты), миграции 148/149 up/down.
- `cargo fmt --all`; `cargo test -p openplotva-llm -p openplotva-app -p openplotva-server
  -p openplotva-web`; `tools/service-smoke.sh`.
- Фронт вручную: lazy-load/Refresh; skeleton/error/empty обоих видов; KPI следуют
  фильтрам; чипы+каунты; provider→model зависимость; errors-toggle; поиск (кириллица);
  анатомия строки для каждого kind + failed + running; open/close детали (кнопка/Esc,
  скролл, .active); дефолтные раскрытия; клик по пипу; клампы текста; expand большого
  tool-result (search/crawl); aux-отступ; sent-маркеры; tech-гриды; lazy raw на огромном
  payload (без фриза); rotated_out; копирования; clear-confirm; полный keyboard-pass;
  светлая тема; ~840px и мобильная ширина.
- Прод после деплоя (Runtime API): llmRuns/REST отдают dialog-run с раундами и
  outcome; song/image-run появляется после генерации; one-off (memory_extraction)
  виден одно-раундовой записью; scrub-воркер логирует чистку; размер
  `pg_total_relation_size('llm_request_events')` под контролем.

## Приложение: незакрытый хвост прежней программы (agentic dialog, Phase 7)

Legacy-деление отложено до ≥2 недель тихого ledger после включения session engine
(включён в проде 2026-07-03 ~14:12 UTC). Grep-аудит перед удалением:
`immediate_tool_answer`, `max_iterations`, `ToolPromptMode::Text`, `use_tool_calls`,
`announce_queue_position`, `InlineSearchAgent`, `run_dialog`, `with_toolbox`,
`RuntimeVirtualSafeToolbox`, guest — ноль хитов или осознанно оставлено. Удаляется:
aifarm-луп и его обвязка, gemini-зеркала, парсеры inline/normalized (+bare при нуле
хитов form=bare в tool_telemetry), no_reply-таксономия до content_blocked, kill-свитчи
`DIALOG_AGENT_LOOP_*`/`DIALOG_SESSION_INJECTION_ENABLED`/`DIALOG_DRAW_UX`,
`DIALOG_AIFARM_USE_TOOL_CALLS`, `LLM_AGENTIC_SEARCH_*`, placeholder-машинерия.
