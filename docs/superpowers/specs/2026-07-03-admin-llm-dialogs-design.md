# HANDOFF: Admin «LLM Dialogs» — редизайн раздела LLM Context

Самодостаточный хендоф для агента без доступа к исходному диалогу. Содержит цель,
всю разведку по коду, решения владельца дословно, спроектированную архитектуру с
обоснованиями, пофазовый план и рабочие конвенции репозитория. Дата фиксации:
2026-07-03. Все `file:line` сверены на эту дату — **перепроверяй перед правкой**,
код живой. Пофазовый план работ: `docs/superpowers/plans/2026-07-03-admin-llm-dialogs.md`.

---

## 1. Продукт и рабочее окружение

- Репозиторий: GitHub `iamwavecut/openplotva`, default branch `main`. Rust-workspace:
  Telegram-бот «Плотва» — мульти-чатовый LLM-персонаж с диалоговым движком,
  генерацией картинок/музыки, памятью, поиском, admin-веб-панелью и Runtime
  GraphQL API.
- Обязательно прочти `AGENTS.md` в корне — там операционные правила (границы
  крейтов, стиль, контракты, секреты). Ключевое для этой задачи повторено в §9.
- Для работы бери свежую ветку от `main` (на момент фиксации main = `6df1d83` +
  возможные новые коммиты).
- Прод задеплоен и живой; деплой — GitHub Actions `deploy-production.yml`
  (workflow_dispatch, авторизован только владелец).

## 2. Предыстория: почему редизайн назрел именно сейчас

2026-07-03 в прод выкачен **dialog session engine** (PR #3 + два фикса-блокера
PR #4, PR #5; включён в проде ~14:12 UTC, флаги по умолчанию on):

- Диалоговый оборот теперь — **агентная сессия**: движок сам крутит цикл
  «LLM-шаг → tool calls → результаты обратно в модель → …» до финального текста
  (`crates/openplotva-app/src/dialog_turn/session.rs`, `run_dialog_session`).
  Один оборот = N LLM-раундов + tool calls + промежуточные сообщения в чат.
- Каждый LLM-шаг идёт через `ChatStepProvider` (одиночный вызов) и роутинговый
  walker; ledger `dialog_turn_outcomes` пишет `detail.iterations` и исходы;
  джоб-события `session_iteration`/`session_tool`/`session_message_sent` пишутся
  в taskman.
- Прежний раздел админки «LLM Context» показывает ПО-СТАРОМУ: плоский поток
  одиночных LLM-запросов. Сессия из 3 раундов выглядит как 3 несвязанные записи
  вперемешку с memory-extraction'ами других чатов. Это и хочет починить владелец.

Отдельный контекст: владелец — единственный оператор; админка — его инструмент
отладки прода. БД недавно распухала до 29GB (инцидент), поэтому любые новые
персистентные данные должны быть ограничены и вычищаемы.

## 3. Задача от владельца (суть его слов)

> Раздел LLM Context переименовать — теперь это **LLM Dialogs**. Он должен
> показывать вызовы агентов со всеми свойствами: провайдер, модель, обстоятельства
> вызова, место вызова, и все раунды запрос-ответ — вся история диалога в одном
> месте, под одной записью. Это stateful-штука, артефакты работы агента. Сейчас
> там низкоуровневая ерунда (запрос-ответ к LLM) — непоказательно и фрагментировано.
> Там, где агентности нет (one-off запросы), — фоллбэк до LLM-запроса, такие
> контексты тоже надо отслеживать. Сам раздел неудобный: в списке видна не вся
> мета — запись не понять, не открыв; collapsed-ноды JSON неудобны; нужен фильтр
> или поиск. Заредизайнь с учётом этой философии.

## 4. Решения владельца (обязательны; результаты 8 уточняющих вопросов)

1. **Скоуп группировки**: ВСЕ многошаговые потоки — dialog-сессии, song/image
   prompt-оптимизаторы (openplotva-agent), console-сессии виртуального диалога,
   будущие лупы. Одношаговые (memory extraction, vision, shield, …) — fallback
   одно-раундовые записи в том же списке.
2. **Хранение**: in-memory кольцо run-записей, ёмче текущего (порядок сотен),
   рестарт = чистый лист, ноль нагрузки на БД. (Выбрал «In-memory, но ёмче».)
3. **Старый плоский вид**: заменить полностью. Один раздел, один ментальный режим.
4. **Live-режим**: только ручной refresh, без поллинга.
5. **Рендер детали** (дословно важное): «Из двух вариантов мне нравится читаемый
   диалог, но у нас там и tool calling, и ответы тулов — нужно красиво и визуально
   структурировать так, чтобы я одним глазом глянул и понял, что происходит в
   диалоге, и при необходимости провалился в самые мелкие детали, вплоть до
   свойств реквестов и респонсов и статистики от сервера LLM. Верхнеуровневая
   визуализация плюс провал вниз по уровням. Дизайн на тебе.» → дизайн делегирован
   исполнителю, спроектирован в §7-Frontend плана.
6. **Raw-тела** (дословно важное): «Скелет диалога можно хранить в памяти, а мясо
   добирать из других мест, где оно прихранено. Если оно не прихранено ещё —
   уточнять, что делать дальше.» → разведка показала: raw-тела НЕ прихранены
   нигде (см. §5.6) → спроектирована ограниченная персистентность (§6.4), это
   и есть ответ «что делать дальше».
7. **Мини-кокпит KPI** сверху — да, сразу.
8. **`run_id` в `llm_request_events`** (существующая аналитическая таблица) — да,
   добавить (аддитивная миграция), чтобы SQL-аналитика могла группировать.

## 5. Результаты разведки (3 Explore-агента + 2 Plan-агента, всё сверено по коду)

### 5.1 Текущий фронтенд раздела

- Вкладка: `web/admin/index.html:58` — `data-tab="llm"`, label «LLM Context»,
  иконка `ti-message-code`; пейн `id="llm"` :837–909; title map :1207
  (`'llm': 'LLM Context'`); хук на вход :1222–1224 (`loadLLMRequests(false)`).
  **`data-tab="llm"` и `id="llm"` при переименовании НЕ менять** — только label и
  title map (command palette строится из живого nav — правок не требует).
- Данные: `GET /admin/api/llm/requests` → `{requests:[...]}` (fetch в
  `loadLLMRequests`, :5002); `POST /admin/api/llm/requests/clear` (:5021). Всё
  через хелпер `apiCall()` (:1258–1293; auth-redirect, `{"error":…}`, лоадер).
- Рендер: JS-блок :4404–5064 + :5271–5299. Сплит-пейн (список 33% + деталь),
  клиентский поиск подстрокой (`llmRequestMatchesQuery`), строка списка =
  `buildLLMRequestItem` (время, модель, type-пилюля, `chat:… msg:… iter:…`,
  превью 160ch). Деталь = метаданные JSON.stringify + «Raw Provider Context» —
  **рукописное eager JSON-дерево** `buildJSONNode` :4849–4904 (все ноды строятся
  сразу → фриз на больших payload; это главная причина «неудобных collapsed-нод»).
- **Pre-existing баг**: кнопка Close зовёт `toggleLLMDetails`, который нигде не
  определён (кидает). Умирает вместе со старым кодом.
- **Анти-паттерн**: `LLM_TYPE_PALETTE` — rgba-литералы прямо в JS (:4409–4418),
  обходит токен-гарды (они проверяют только markup/CSS). Выпилить.

### 5.2 Дизайн-система админки (обязательна к использованию)

- `web/admin/components.js` (~1300 строк): кастом-элементы `pl-button`,
  `pl-button-group`, `pl-input`, `pl-textarea`, `pl-select`, `pl-field-group`,
  `pl-toggle`, `pl-table` (columns/rows/state/emptyTitle/onRetry, `pl:row-click`),
  `pl-modal`, `pl-toast-host`, `pl-slider`, `pl-drawer` (атрибут `open`, Esc →
  `pl:close`; **полноширинный normal-flow блок**, прецедент Memory/Routing),
  `pl-graph`, `pl-timeline`, `pl-slotbar`, `pl-flow`, `pl-diff-list`.
  Хелперы: `PL.toast/alert/confirm/skeleton/skeletonTable/empty/error/badge/el/text`.
- Событийная модель: ТОЛЬКО делегирование `data-action="fnName"` +
  `data-args='[...]'` (+ `data-confirm`); никаких inline-обработчиков.
- `web/admin/tokens.css` (~370 строк): spacing `--sp-*`, radius, типографика,
  семантические цвета `--c-*`, категориальная палитра `--c-cat-0..63`
  (golden-angle, стабильные цвета сущностей), доменные палитры
  (`--c-cardtype-*`, `--c-relation-*`, `--c-visibility-*`, `--c-ent-*`,
  `--c-role-*`, `--c-slot-*`, `--c-log-*`, `--c-status-*`, `--c-json-*`),
  motion/elevation/z-ladder; светлая тема `[data-theme="light"]` (только цвета).
- CSS-примитивы: `.metric-card` (KPI-плитки), `.facet-bar` + `.filter-chip`
  (фасеты Memory), `.list-group`/`.list-item` (общие со вкладкой Safety —
  сохранить), `.json-tree*`/`.json-node*` (admin.css:831–938 — generic, оставить),
  `.mp-*` вью-табы Routing Ops.
- **Guard-механика**: ассеты вшиты `include_bytes!` в
  `crates/openplotva-web/src/lib.rs` (константы sha256 :35–75; index.html :70,
  admin.css :46, tokens.css :40, components.css :52, components.js :58).
  Тесты :488–556: (а) хеши совпадают; (б) в admin-разметке запрещены сырые
  `<button>/<input>/<select>/<textarea>/<table>`, `onclick=`/`onsubmit=`/`style=`,
  `alert()/confirm()`; (в) в admin.css/components.css запрещены цветовые литералы
  (только `var(--c-*)`). После правки любого ассета: пересчитать
  `shasum -a 256 web/admin/<f>` → обновить константу → `cargo test -p openplotva-web`
  (упавший тест печатает ожидаемый хеш). Перед мерджем UI-правок прогнать скилл
  `openplotva-design-system-review`.

### 5.3 Бэкенд-пайплайн LLM-трейсов (существующий)

- Типы/реестр: `crates/openplotva-llm/src/trace.rs` (~191 строка) —
  `LlmCallObserver` (trait), `LlmCallContext` (chat/thread/message/user…),
  `LlmCallTags` (provider/source/flow/mode/request_kind/iteration/docs_chars),
  `LlmCallRecord`, `LlmCallTraceRegistry` (глобальный `OnceLock`, :73–134;
  `observe(record)` форвардит в единственного зарегистрированного обсервера).
  Низкоуровневые клиенты (aifarm, gemini) эмитят сюда сами: диалоговые шаги
  aifarm.rs ~:2660,2754 (несут `iteration` из `ChatStepRequest`), aux-вызовы
  (memory extraction aifarm.rs:1749, gemini aux :3185–3203).
- Приёмник: `crates/openplotva-app/src/runtime_llm.rs` (~1475 строк) —
  `RuntimeLlmObserver` (:473–498) → (а) in-memory кольцо `RuntimeLlmTraceBuffer`
  ёмкостью 1000 (константа `GO_LLM_TRACE_BUFFER_CAPACITY`, env-ручки НЕТ; wiring
  lib.rs ~:9800; `record()` сейчас НЕ возвращает присвоенный id — придётся
  вернуть); (б) async mpsc (10k) → батч-писатель (100/5с) в Postgres
  `llm_request_events`.
- Таблица `llm_request_events` (миграции 25 + 36 + 120): метаданные
  (source/flow/provider/request_kind/chat_id/thread_id/message_id/user_id/model/
  iteration/prompt_chars/duration_ms/error), usage-токены, провайдерские тайминги
  (prompt_eval/generation ms+tps), сэмплинг-конфиг, `inference_params JSONB`.
  **Raw-тел НЕТ** (см. §5.6). Есть ролл-ап в агрегаты + cleanup-воркер
  (`run_llm_request_event_cleanup_worker_until`, runtime_llm.rs:717–762, интервал
  7 дней, батчи 10k) — образец для scrub-воркера.
- Экспозиция: Runtime GraphQL `llmRequests(filter)` (openplotva-server/src/
  runtime_graphql.rs:1467–1478; `RuntimeLlmRequestData` :854–882; фильтр
  source/model/chatID/userID/messageID/errorOnly/emptyOnly/q/limit) — **это
  операторский контракт, менять нельзя**. Admin REST `GET /admin/api/llm/requests`
  (lib.rs, маршруты :730–734, хендлеры :1019–1034 → :6854–6909) — потребители
  ТОЛЬКО сама вкладка + playwright-смок `tools/service-smoke.web-ui.spec.js:387,445`
  → можно вывести из эксплуатации вместе с фронтом (смок переписать).
- Ledger оборотов: `dialog_turn_outcomes` (миграция 144;
  dialog_turn/ledger.rs — кольцо `RuntimeTurnOutcomeBuffer` 2048 + async-писатель;
  outcome/reason/provider/elapsed_ms/user_signal/sent_message_parts/
  side_effect_ticket_id/detail JSONB c iterations). `DialogTurnObserver::record`
  зовётся РОВНО один раз на оборот из finalize (engine.rs:602–612) — идеальная
  точка обогащения run-записи исходом, не ломает single-exit `finalize_turn`.
- Тул-коллы сессии перситстятся в chat history (`persist_dialog_tool_calls`,
  dialog_jobs/input.rs) и в джоб-события (`append_session_tool_event`,
  session.rs:~608–616 — рядом с ним же встраивается `record_tool_result`).

### 5.4 Место вызова (call-site taxonomy, значения `flow` на трейсах)

`dialog` (сессии и legacy-диалог), `memory_extraction`, `vision`, `shield`,
`history_summary` и пр. one-off'ы; console-вызовы идут через тот же step-провайдер.
Song/image оптимизаторы flow не имеют, потому что вообще не трейсятся (§5.6).

### 5.5 Прецеденты (следовать им)

- **Admin REST**: маршруты в `install_static_web_routes` (lib.rs:700–793 +
  список `GO_ADMIN_API_ROUTE_PATTERNS` :797–841 + parity-тест :15079–15129 —
  новые роуты добавлять В ОБА МЕСТА). Auth: `require_admin_request(headers,
  admin_ids, secret)` (подписанная кука/`X-Telegram-User-ID`). Ответы:
  `admin_json_response`/`admin_json_no_cache_response`, ошибки `{"error":"…"}`,
  пагинация limit-only (`ADMIN_PAGE_LIMIT` 1000). Образцы: memory cards
  (:3853–3885), routing snapshot/status (:8323–8456; паттерн «тяжёлый snapshot +
  лёгкий status»).
- **Memory-редизайн** (свежий, самый близкий прецедент): спека
  `docs/superpowers/specs/2026-06-24-admin-memory-redesign-design.md` + план в
  `docs/superpowers/plans/` (конвенция имён `YYYY-MM-DD-<topic>-design.md`).
  UI-паттерн: KPI-кокпит → фасетный Explorer → деталь; фасет-чипы; pl-drawer.
- **Routing Ops**: вью-табы `.mp-viewtabs`, `mpState` + императивные рендеры,
  стабильный hash → `--c-cat-N` для цветов сущностей (index.html:1588–1611) —
  переиспользовать для цветов моделей.

### 5.6 Критические находки (поправки к любой интуиции)

1. **Song/image оптимизаторы НЕ трейсятся вовсе**: `build_request`
   (agent_runtime.rs:1239–1274) не ставит `request.trace`, а
   `AifarmHttpClient::emit_call_trace` ранне-выходит при `None` (aifarm.rs:725–727).
   Проверено грепом: в agent_runtime.rs ноль упоминаний `request.trace`. Их надо
   начать трейсить — это самостоятельный observability-win.
2. **Raw-тела запросов/ответов не персистятся никуда**: INSERT-список
   `llm_request_events` (runtime_llm.rs:32–71) их не содержит; живут только в
   кольце на 1000 и теряются при ротации/рестарте.
3. **Прецедент сжатия в этом репо**: миграция `139_whitecircle_checks_lz4` —
   Postgres column-level `SET COMPRESSION lz4` на больших JSONB. App-side gzip
   не нужен.
4. **Task-local безопасен на LLM-путях**: единственные spawn'ы рядом — WhiteCircle
   pre-tool аудит (whitecircle.rs:930 — не производит LlmCallRecord, потеря скоупа
   безвредна) и waiter капасити-пула (не LLM-вызов). Walker бежит инлайн —
   доказано работающим task-local `TURN_DEADLINE` (dialog_turn/budget.rs:15–20,
   session.rs:300–310 — бери за образец).
5. **Vision/media-материализация — внутри `execute_dialog_turn`**
   (engine.rs:123–125): скоуп вокруг всего turn (а не только сессии) ловит и эти
   вложенные aux-вызовы.
6. **`DialogJobParams.message_text`** (openplotva-taskman/src/lib.rs:352) —
   источник trigger-превью для origin run-записи.
7. Последняя миграция — **147** → новые: 148, 149. Мигратор:
   `openplotva-storage/src/lib.rs:1405` (`sqlx::migrate!("../../migrations")`).
8. Кросс-сигнатурные правки (все внутри openplotva-app):
   `RuntimeLlmTraceBuffer::record → i64`, `RuntimeLlmObserver::new` (+1 арг),
   `DialogTurnObserver::new` (+1 арг), `PostgresRuntimeLlmEventRecorder::spawn`
   (+policy).

## 6. Ключевые архитектурные решения и ПОЧЕМУ

1. **Корреляция через tokio task-local `LlmRunScope`, штамп в
   `LlmCallTraceRegistry::observe`** (а не протаскивание run_id через структуры
   запросов). Почему: одна точка покрывает все emit-сайты без правки клиентов;
   протаскивание через `ChatStepRequest` пропустило бы vision-материализацию,
   legacy-путь, agent-`ReasonerCall` и console, и раздуло бы публичные типы
   openplotva-dialog. Риск спавнов проверен (§5.6.4). Границы крейтов: тип скоупа
   владеет openplotva-llm, ставит openplotva-app (зеркало TURN_DEADLINE).
2. **Закрытие dialog-run через sink в `DialogTurnObserver::record`** (а не хук в
   `finalize_turn`). Почему: record зовётся ровно один раз на оборот, single-exit
   контракт finalize не трогается; `job_id` — ключ джойна (run_id = `"job-{id}"`);
   `finish_run` — no-op для незнакомых id, так что merged/parked/skipped обороты
   фантомных записей не создают.
3. **One-off вызовы — записи в ТОМ ЖЕ буфере** (создаются на observe, kind=flow),
   а не синтез на лету при чтении. Почему: единый список, единый формат, нет
   ленивой логики в хендлере.
4. **«Мясо»**: скелет (метаданные + тексты ответов cap 8000 + тул-коллы) в
   run-буфере; полный raw резолвится в детали по цепочке: кольцо трейсов по
   `trace_id` (`raw_source:"live"`) → БД по `(run_id, run_seq)` → честный
   `"rotated_out"`. Чтобы БД-фоллбэк работал, raw-тела НАЧИНАЮТ персиститься в
   `llm_request_events` (lz4-колонки) с жёсткими рамками: cap 64KB/тело,
   отдельный scrub-воркер NULL-ит тела старше 48ч (сама строка живёт по прежней
   ретенции). Матожидание объёма: при ~20k вызовов/сутки и lz4 ~4–8× —
   ~0.3–0.6GB steady, worst ≤5GB, лечится ручками/выключателем. Почему так:
   выполняет пожелание «мясо добирать из мест, где прихранено», не повторяя
   29GB-инцидент; `run_seq` нужен потому, что in-memory trace_id обнуляется
   рестартом — БД-ключ должен быть стабильным.
5. **KPI считаются клиентски** из загруженного списка (он ограничен сотнями
   скелетов) — отдельный stats-эндпоинт не оправдан.
6. **GraphQL `llmRuns` — аддитивно, скелеты без raw** (для удалённой диагностики,
   которой владелец активно пользуется). `llmRequests` заморожен байт-в-байт.
7. **Clear чистит только run-буфер**, кольцо трейсов не трогает (его читает
   GraphQL llmRequests).
8. **Фронт: ноль новых pl-компонентов** — всё из существующих pl-* + CSS-классы
   `.llmd-*` (прецедент: весь Routing Ops и Memory сделаны так). Почему: новые
   компоненты = хеш-чурн components.js/css + ARIA-контракты + докучность, а
   переиспользуемость сомнительна.
9. **Деталь — полноширинный `pl-drawer`, замещающий список** (не сплит-пейн).
   Почему: raw-payload'ам и карточкам раундов нужна вся ширина (теснота — главная
   боль старой детали); Esc/`pl:close` бесплатно; выпиливается резайзер и класс
   багов `toggleLLMDetails`.
10. **JSON-дерево — переписать на ленивое** (дети рендерятся при первом
    раскрытии), сохранив CSS `.json-node*` и хелперы
    (`jsonNodeKind/Summary`, `isExpandableJSONString`, `bindJSONTree`). Лечит
    фриз на огромных payload — корень жалобы на collapsed-ноды.

## 7. План работ

Полный пофазовый план (A: корреляция+миграция 148 → B: run-буфер → C:
raw-персистентность+REST+GraphQL → D: фронтенд-свап) — в
`docs/superpowers/plans/2026-07-03-admin-llm-dialogs.md`. Каждая фаза —
независимо-шипуемый PR.

## 8. Верификация

Бэкенд (юнит-матрица):
- скоуп: записи внутри `with_run_scope` несут его, вне — None; уже проставленный
  не перезатирается; две параллельные задачи не бликуют;
- aux: внутри dialog-run раунд с flow=memory_extraction → is_aux=true;
- буфер: begin→rounds(seq 1..n)→tools→sent→finish; finish незнакомого — no-op;
  one-off → закрытая запись; re-begin → Abandoned старого; watchdog; порядок
  вытеснения кольца; prune_chat; clear;
- fan-out: id кольца == round.trace_id; run_seq попадает в DB-строку; unscoped →
  one-off;
- ledger-обогащение: финализация закрывает `job-{id}` с outcome; merged/parked
  runs не создают; **луп-левел тест на прокидку llm_runs через воркер-опции**
  (урок PR #5 — воркер-луп уже однажды молча ронял опцию session);
- трейс-тег оптимизаторов (регрессия «не трейсились»);
- writer SQL: колонки run_id/run_seq/raw; cap → NULL; выключено → NULL;
- scrub SQL: только строки с телами, только старше cutoff, UPDATE не DELETE;
- REST: 403 без auth; method-гварды; фильтры; список без тел; деталь
  live|db|rotated_out; clear не трогает кольцо; parity-тест;
- GraphQL: SDL llmRequests заморожен; llmRuns отдаёт скелеты;
- миграции 148/149 up/down идемпотентны.

Команды: `cargo fmt --all`; `cargo test -p openplotva-llm -p openplotva-app
-p openplotva-server -p openplotva-web`; `tools/service-smoke.sh`.

Фронт (ручной чек-лист): lazy-load/Refresh; все skeleton/empty/error; KPI следуют
фильтрам; чипы+каунты+single-select; provider→model; errors-toggle; поиск
(кириллица, Enter); анатомия строки на каждый kind + failed + running; деталь
open/close (кнопка/Esc, скролл-restore, .active); дефолтные раскрытия; пип →
скролл; клампы; expand большого tool-result (search/crawl payload); aux-отступ;
sent-маркеры; tech-гриды; ленивое raw-дерево на огромном payload без фриза;
rotated_out; все копирования (toast); clear-confirm; полный keyboard-pass
(строки/чипы/пипы, фокус-кольца); светлая тема; ~840px и мобильная ширина.

Прод после деплоя (Runtime API): REST/llmRuns отдают dialog-run с раундами и
outcome; song/image-run появляется после генерации; one-off (memory_extraction)
виден одно-раундовой записью; scrub-воркер логирует чистку;
`pg_total_relation_size('llm_request_events')` под контролем.

## 9. Рабочие конвенции и грабли (обязательны)

1. **Git/PR**: ветки и коммиты БЕЗ самоатрибуции ИИ. Коммиты — английские,
   содержательные, в стиле репо («Forward the session option through the dialog
   worker loop»). PR: `gh pr create`; на PR автоматически прибегает ревью-бот
   **Qodo** — его комментарии ЧИТАТЬ: багрепорты часто валидные (он поймал
   реальный баг в PR #4). Согласен — чини и отвечай «fixed in <sha>»; не согласен
   — аргументированно отвечай и резолвь тред (GraphQL
   `addPullRequestReviewThreadReply` + `resolveReviewThread`). CI-джобы: Rust
   workspace (5–9 мин, непредсказуемо), CodeQL, Semgrep ×2, Container image,
   Rust dependencies. **Опрашивать `gh pr checks` раз в 60с фоновым циклом до
   устаканивания** (не фиксированные слипы). Мердж: `gh pr merge N --merge`
   (merge-коммит; сохраняет фазовые коммиты).
2. **Деплой**: `gh workflow run deploy-production.yml --ref main` → опрос
   `gh run view <id>` раз в 60с (сборка+выкат ~8 мин). Тег прод-образа = SHA
   коммита. Деплоить только когда владелец попросил.
3. **Секреты**: НИКОГДА не коммитить токены/ключи/`.env`/адреса внутренней
   инфраструктуры. Доступ к прод-хосту и Runtime API (адрес, транспорт,
   debug-токен с коротким TTL) выдаёт владелец per-session — спроси у него и не
   персисти нигде, включая доки и коммиты.
4. **Runtime API** (диагностика прода): GraphQL-эндпоинт (доступ — см. п.3).
   Полезные query: `llmRequests(filter:{limit:N})`, `dialogTurnOutcomes`,
   `dispatcherSendFailures`, `healthSnapshot { db{status} redis{status}
   dispatcher{status} updatesQueueLength }`, `logs(afterSeq, level:"warn",
   search)` (время — epoch millis; буфер начинается с рестарта), `sqlRead(input:
   {sql:"…"})` — read-only SQL прямо в прод-БД (`SqlReadInput!` с полем `sql`;
   типы уточняй интроспекцией). `configSnapshot` может отдавать null на
   debug-токене — не рассчитывай.
5. **Локальный тулчейн**: rustup НЕТ; Homebrew rustc 1.96.0, а CI держит clippy
   1.95 c `-D warnings` — локальный clippy шумит лишними линтами в чужом коде
   (collapsible_if и пр.). Правило: доказывай чистоту СВОЕГО диффа
   (`cargo clippy -p <crate> --all-targets` + grep по своим файлам), не мира.
   Docker локально может лежать — контейнер-смоки не прогнать, скажи об этом в
   отчёте. Всегда `cargo fmt --all` после правок Rust.
6. **Диск/сборки**: машина близка к заполнению — не запускай параллельные
   full-workspace сборки в нескольких worktree; у каждого worktree свой target
   (не шарить CARGO_TARGET_DIR — фантомные ошибки со stale .rlib).
7. **Web-ассеты**: любой изменённый файл в `web/` → пересчитать sha256-константу
   в `crates/openplotva-web/src/lib.rs` → `cargo test -p openplotva-web`. Гварды
   запрещают сырые контролы/inline-стили/цветолитералы (§5.2). Перед мерджем
   admin-UI — скилл `openplotva-design-system-review`.
8. **Грязные изменения** в рабочей копии владельца — норма; не reset/clean.
   Prompt-файлы (`prompts/*.prompt`) — кэш-чувствительные контракты.
9. **Definition of done**: все 4 фазы смержены с зелёным CI и нулём нерезолвленных
   Qodo-тредов; прод задеплоен; прод-проверки §8 пройдены; старый эндпоинт и
   мёртвый JS удалены (Phase D).

## 10. Вне скоупа этой задачи (не трогать, но знать)

- **Phase 7 прежней agentic-программы** (удаление legacy-лупа aifarm/gemini,
  парсеров, kill-свитчей `DIALOG_AGENT_LOOP_*`, `DIALOG_DRAW_UX`,
  `DIALOG_AIFARM_USE_TOOL_CALLS`, `LLM_AGENTIC_SEARCH_*`) — отложено до ≥2 недель
  тихого прод-ledger ПОСЛЕ 2026-07-03. Не удаляй ничего из legacy-диалогового
  пути в рамках LLM Dialogs.
- GraphQL `llmRequests` и его REST-собратья вне админ-вкладки — контракты.
- Settings WebApp (`web/settings/`) — отдельная граница, не трогать.
