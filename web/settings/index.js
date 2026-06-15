(() => {
  const tgCtx = initTelegram();
  const query = new URLSearchParams(window.location.search);
  const signature = query.get('signature') || '';
  const userIdFromQuery = parseIntID(query.get('user_id'));
  const chatIdFromQuery = parseIntID(query.get('chat_id'));
  const modeFromQuery = (query.get('mode') || '').trim();
  const CUSTOM_PERSONA_MAX_CHARS = 1000;

  const landingPageContent = `
    <div class="page" data-name="landing">
      <div class="navbar">
        <div class="navbar-bg"></div>
        <div class="navbar-inner">
          <div class="title">Настройки</div>
        </div>
      </div>
      <div class="page-content">
        <div class="block-title">Плотва</div>

        <div class="card card-outline card-link">
          <a href="/chats/" class="card-content card-content-padding settings-tile-link" id="tileChats">
            <div class="display-flex align-items-center justify-content-space-between">
              <div class="display-flex align-items-center" style="gap: 12px; min-width: 0;">
                <div style="font-size: 28px; line-height: 1; flex: 0 0 auto;">💬</div>
                <div style="min-width: 0;">
                  <div style="font-size: 17px; font-weight: 600; line-height: 1.2;">Настройки чатов</div>
                  <div class="text-color-gray" style="margin-top: 2px; font-size: 14px; line-height: 1.25;">
                    Управление чатами и группами которыми Вы владеете или управляете
                  </div>
                </div>
              </div>
              <div style="opacity: 0.35; font-size: 22px; line-height: 1; flex: 0 0 auto;">›</div>
            </div>
          </a>
        </div>

        <div class="card card-outline card-link">
          <a href="/general/" class="card-content card-content-padding settings-tile-link" id="tileUserSettings">
            <div class="display-flex align-items-center justify-content-space-between">
              <div class="display-flex align-items-center" style="gap: 12px; min-width: 0;">
                <div style="font-size: 28px; line-height: 1; flex: 0 0 auto;">👤</div>
                <div style="min-width: 0;">
                  <div style="font-size: 17px; font-weight: 600; line-height: 1.2;">Настройки пользователя</div>
                  <div class="text-color-gray" style="margin-top: 2px; font-size: 14px; line-height: 1.25;">
                    Тон общения, персона и другое для Вас: личный чат с ботом и глобальные настройки
                  </div>
                </div>
              </div>
              <div style="opacity: 0.35; font-size: 22px; line-height: 1; flex: 0 0 auto;">›</div>
            </div>
          </a>
        </div>

        <div class="block-footer" id="signatureWarning" style="display:none;">
          Подпись (signature) не найдена — проверьте ссылку на настройки.
        </div>
        <div class="block block-strong inset" id="debugBlock" style="display:none;">
          <div class="text-color-gray" style="font-size: 13px;">
            <strong>Debug</strong><br>
            <span id="debugInfo"></span>
          </div>
        </div>
      </div>
    </div>
  `;

  const chatsPageContent = `
    <div class="page" data-name="chats">
      <div class="navbar">
        <div class="navbar-bg"></div>
        <div class="navbar-inner sliding">
          <div class="left">
            <a href="#" class="link" id="backLink">
              <i class="icon icon-back"></i>
              <span class="if-not-md">Назад</span>
            </a>
          </div>
          <div class="title">Чаты</div>
        </div>
      </div>
      <div class="page-content">
        <div class="block-title">Выберите чат для настройки</div>
        <div class="list list-strong inset list-dividers-ios list-outline-ios">
          <ul id="chatList"></ul>
        </div>
        <div class="block block-strong inset" id="emptyBlock" style="display:none;">
          <div class="text-color-gray">Нет доступных чатов</div>
        </div>

        <div class="card card-outline card-expandable chat-missing-help-card">
          <div class="card-content">
            <div class="card-content-padding card-open" data-card=".chat-missing-help-card" style="cursor: pointer;">
              <div class="display-flex align-items-center justify-content-space-between">
                <div class="display-flex align-items-center" style="gap: 12px; min-width: 0;">
                  <div style="font-size: 26px; line-height: 1; flex: 0 0 auto;">❓</div>
                  <div style="min-width: 0;">
                    <div style="font-size: 17px; font-weight: 600; line-height: 1.2;">Почему я не вижу свой чат?</div>
                    <div class="text-color-gray" style="margin-top: 2px; font-size: 14px; line-height: 1.25;">Нажмите, чтобы открыть инструкцию</div>
                  </div>
                </div>
                <div style="opacity: 0.35; font-size: 22px; line-height: 1; flex: 0 0 auto;">›</div>
              </div>
            </div>

            <div class="card-content-padding card-opened-fade-in">
              <div class="display-flex justify-content-flex-end" style="margin-bottom: 12px;">
                <a href="#" class="link card-close" data-card=".chat-missing-help-card">Закрыть</a>
              </div>
              <p>Чат появляется в списке настроек, если вы являетесь владельцем (создателем) чата, администратором с правом назначать других администраторов (Promotion) или назначенным заместителем.</p>
              <p>Из-за ограничений Telegram Bot API Плотва по умолчанию не всегда знает, кто является владельцем чата. Она узнаёт ваш статус только когда получает обновления прав.</p>
              <p>Если вы подходите под условия, но чат не отображается, нужно обновить ваш статус. Для этого в самом чате вызовите команду <pre>/settings</pre>.</p>
              <p>Команду должен отправить владелец или администратор с правом на Promotion. Отправка должна быть от имени пользователя: не анонимно и не «от имени группы».</p>
              <p>После этого перейдите в настройки и обновите список — Плотва обновит права и покажет чат.</p>
              <p>Если вы владелец чата, вы можете назначить заместителей прямо в настройках чата. Заместители получают доступ ко всем настройкам Плотвы, кроме управления списком заместителей.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;

  const settingsPageContent = `
    <div class="page" data-name="settings">
      <div class="navbar">
        <div class="navbar-bg"></div>
        <div class="navbar-inner sliding">
          <div class="left">
            <a href="#" class="link" id="backLink">
              <i class="icon icon-back"></i>
              <span class="if-not-md">Назад</span>
            </a>
          </div>
          <div class="title" id="pageTitle">Настройки</div>
        </div>
      </div>

      <div class="toolbar toolbar-bottom toolbar-bottom-ios toolbar-bottom-md" id="saveToolbar" style="display:none;">
        <div class="toolbar-inner">
          <a href="#" class="link color-red" id="cancelButton">Отменить</a>
          <a href="#" class="link" id="saveButton">Сохранить</a>
        </div>
      </div>

      <div class="page-content" id="pageContent">
        <form id="settingsForm">
          <div class="block-title">Основные настройки</div>
          <div class="list list-strong inset list-dividers-ios list-outline-ios">
            <ul>
              <li>
                <div class="item-content">
                  <div class="item-inner">
                    <div class="item-title">💬 Плотва включена<div class="item-footer">Глобальный выключатель</div></div>
                    <div class="item-after">
                      <label class="toggle toggle-init">
                        <input type="checkbox" name="enable_global_text_reply">
                        <span class="toggle-icon"></span>
                      </label>
                    </div>
                  </div>
                </div>
              </li>
              <li>
                <div class="item-content">
                  <div class="item-inner">
                    <div class="item-title">🎨 Плотва рисует</div>
                    <div class="item-after">
                      <label class="toggle toggle-init">
                        <input type="checkbox" name="enable_global_draw_reply">
                        <span class="toggle-icon"></span>
                      </label>
                    </div>
                  </div>
                </div>
              </li>
              <li id="disableRandomReactivityItem">
                <div class="item-content">
                  <div class="item-inner">
                    <div class="item-title">🙈 Не реагировать на меня<div class="item-footer">Выключить случайные срабатывания на мои сообщения в группах. На личные сообщения, ответы и цитирование - не влияет</div></div>
                    <div class="item-after">
                      <label class="toggle toggle-init">
                        <input type="checkbox" name="disable_random_reactivity">
                        <span class="toggle-icon"></span>
                      </label>
                    </div>
                  </div>
                </div>
              </li>
              <li id="hideOriginalDrawPromptItem">
                <div class="item-content">
                  <div class="item-inner">
                    <div class="item-title">🕶️ Скрывать оригинальный промпт<div class="item-footer" id="hideOriginalDrawPromptNotice">Доступно только VIP-подписчикам</div></div>
                    <div class="item-after">
                      <label class="toggle toggle-init" id="hideOriginalDrawPromptToggle">
                        <input type="checkbox" name="hide_original_draw_prompt">
                        <span class="toggle-icon"></span>
                      </label>
                    </div>
                  </div>
                </div>
              </li>
            </ul>
          </div>

          <div id="detailsSection">
            <div class="block-title">Настроение</div>
            <div class="list list-strong inset list-dividers-ios list-outline-ios">
              <ul>
                <li class="settings-picker-item">
                  <div class="item-content item-input">
                    <div class="item-inner">
                      <div class="item-title item-label">Тон общения</div>
                      <div class="item-input-wrap">
                        <input class="settings-picker-input" type="text" id="moodAlignmentPickerInput" readonly placeholder="Выберите">
                        <input type="hidden" name="mood_alignment">
                      </div>
                    </div>
                  </div>
                </li>
              </ul>
            </div>

            <div class="block-title">Персона бота</div>
            <div class="list list-strong inset list-dividers-ios list-outline-ios">
              <ul>
                <li class="item-content item-input item-input-with-info item-input-with-error-message" id="customPersonaItem">
                  <div class="item-inner">
                    <div class="item-title item-label">Описание</div>
                    <div class="item-input-wrap">
                      <textarea class="resizable" name="custom_persona" placeholder="Описание персоны..." rows="4"></textarea>
                      <div class="item-input-info" id="personaCounter"></div>
                      <div class="item-input-error-message" id="personaError"></div>
                    </div>
                  </div>
                </li>
              </ul>
            </div>

            <div class="block-title">Общие настройки</div>
            <div class="list list-strong inset list-dividers-ios list-outline-ios">
              <ul>
                <li>
                  <div class="item-content">
                    <div class="item-inner">
                      <div class="item-title">🤬 Плотва ругается</div>
                      <div class="item-after">
                        <label class="toggle toggle-init">
                          <input type="checkbox" name="enable_profanity">
                          <span class="toggle-icon"></span>
                        </label>
                      </div>
                    </div>
                  </div>
                </li>
                <li>
                  <div class="item-content">
                    <div class="item-inner">
                      <div class="item-title">🔄 Плотва хуифицирует<div class="item-footer">Периодически вкидывать в чат ковёрканные-шмовёрканные слова-пиздава</div></div>
                      <div class="item-after">
                        <label class="toggle toggle-init">
                          <input type="checkbox" name="enable_obscenifier">
                          <span class="toggle-icon"></span>
                        </label>
                      </div>
                    </div>
                  </div>
                </li>
              </ul>
            </div>

            <div id="groupSettingsSection">
              <div class="block-title">Настройки групповых чатов</div>
              <div class="list list-strong inset list-dividers-ios list-outline-ios">
                <ul>
                  <li class="settings-picker-item">
                    <div class="item-content item-input">
                      <div class="item-inner">
                        <div class="item-title item-label">Реактивность</div>
                        <div class="item-input-wrap">
                          <input class="settings-picker-input" type="text" id="reactivityPickerInput" readonly placeholder="Выберите">
                          <input type="hidden" name="reactivity_percentage">
                        </div>
                      </div>
                    </div>
                  </li>
                  <li class="feature-hidden" data-feature="greetings">
                    <div class="item-content">
                      <div class="item-inner">
                        <div class="item-title">👋 Плотва приветствует</div>
                        <div class="item-after">
                          <label class="toggle toggle-init">
                            <input type="checkbox" name="enable_greet_joiners">
                            <span class="toggle-icon"></span>
                          </label>
                        </div>
                      </div>
                    </div>
                  </li>
                  <li>
                    <div class="item-content">
                      <div class="item-inner">
                        <div class="item-title">🎲 Игра дня включена</div>
                        <div class="item-after">
                          <label class="toggle toggle-init">
                            <input type="checkbox" name="enable_daily_game">
                            <span class="toggle-icon"></span>
                          </label>
                        </div>
                      </div>
                    </div>
                  </li>
                  <li class="settings-picker-item">
                    <div class="item-content item-input">
                      <div class="item-inner">
                        <div class="item-title item-label">Тема игры</div>
                        <div class="item-input-wrap">
                          <input class="settings-picker-input" type="text" id="dailyGameThemePickerInput" readonly placeholder="Выберите">
                          <input type="hidden" name="daily_game_theme">
                        </div>
                      </div>
                    </div>
                  </li>
                </ul>
              </div>
            </div>

            <div id="deputiesSection" style="display:none;">
              <div class="block-title">Заместители</div>
              <div class="list list-strong inset list-dividers-ios list-outline-ios">
                <ul>
                  <li>
                    <a href="#" id="deputiesAutocompleteOpener" class="item-link item-content">
                      <div class="item-inner">
                        <div class="item-title">Управление заместителями<div class="item-footer deputies-summary" id="deputiesSummary">Выберите участников чата, которые смогут управлять настройками Плотвы.</div></div>
                        <div class="item-after" id="deputiesCount">0</div>
                      </div>
                    </a>
                  </li>
                </ul>
              </div>
              <div class="block block-strong inset" id="deputiesPreviewWrap" style="display:none;">
                <div class="deputies-preview" id="deputiesPreview"></div>
              </div>
              <div class="block-footer">
                Заместители получают доступ ко всем настройкам Плотвы в этом чате, кроме управления списком заместителей.
              </div>
            </div>

            <div class="block block-strong inset" id="privateInfoSection" style="display:none;">
              <p>ℹ️ В приватных чатах бот всегда реактивен и отвечает на все сообщения.</p>
            </div>
          </div>
        </form>
        <div class="block-title" id="memoryTitle">Память</div>
        <div class="list list-strong inset list-dividers-ios list-outline-ios media-list" id="memoryListBlock" style="display:none;">
          <ul id="memoryList"></ul>
        </div>
        <div class="block block-strong inset" id="memoryEmpty" style="display:none;">
          <div class="text-color-gray">Памяти пока нет</div>
        </div>
      </div>
    </div>
  `;

  const notFoundPageContent = `
    <div class="page" data-name="not-found">
      <div class="navbar">
        <div class="navbar-bg"></div>
        <div class="navbar-inner">
          <div class="title">Настройки</div>
        </div>
      </div>
      <div class="page-content">
        <div class="block block-strong inset">
          <div class="text-color-gray">Страница не найдена</div>
          <div style="margin-top: 12px;">
            <a href="/" class="button button-fill">На главную</a>
          </div>
        </div>
      </div>
    </div>
  `;

  const store = Framework7.createStore({
    state: {
      signature: signature || null,
      userId: tgCtx.userId ?? userIdFromQuery,
      chats: [],
      lastSettings: null,
    },
    actions: {
      setUserId({ state }, userId) {
        state.userId = userId;
      },
      setSignature({ state }, signatureValue) {
        state.signature = signatureValue;
      },
      async loadChats({ state }) {
        const userId = state.userId;
        if (!userId) {
          throw new Error('missing user_id');
        }
        if (!state.signature) {
          throw new Error('missing signature');
        }
        const chats = await apiGetJSON('/api/chats', {
          user_id: String(userId),
          signature: state.signature,
        });
        state.chats = Array.isArray(chats) ? chats : [];
      },
      async loadSettings({ state }, { chatId }) {
        if (!state.signature) {
          throw new Error('missing signature');
        }
        const params = {
          chat_id: String(chatId),
          signature: state.signature,
        };
        if (state.userId) {
          params.user_id = String(state.userId);
        }
        const settings = await apiGetJSON('/api/settings', params);
        state.lastSettings = settings || null;
        return settings;
      },
      async saveSettings({ state }, payload) {
        if (!state.signature) {
          throw new Error('missing signature');
        }
        await apiRequestJSON('/api/settings', {
          method: 'PUT',
          body: payload,
        });
      },
      async loadDeputyCandidates({ state }, { chatId, query, limit }) {
        if (!state.signature) {
          throw new Error('missing signature');
        }
        if (!state.userId) {
          throw new Error('missing user_id');
        }
        const res = await apiGetJSON('/api/settings/deputies/candidates', {
          chat_id: String(chatId),
          user_id: String(state.userId),
          signature: state.signature,
          query: String(query || ''),
          limit: String(limit || 50),
        });
        return Array.isArray(res?.items) ? res.items : [];
      },
      async saveDeputies({ state }, payload) {
        if (!state.signature) {
          throw new Error('missing signature');
        }
        return apiRequestJSON('/api/settings/deputies', {
          method: 'PUT',
          body: payload,
        });
      },
      async loadMemory({ state }, { chatId, limit }) {
        if (!state.signature) {
          throw new Error('missing signature');
        }
        const params = {
          chat_id: String(chatId),
          signature: state.signature,
        };
        if (state.userId) {
          params.user_id = String(state.userId);
        }
        if (limit) {
          params.limit = String(limit);
        }
        return apiGetJSON('/api/settings/memory', params);
      },
      async deleteMemory({ state }, { chatId, id }) {
        if (!state.signature) {
          throw new Error('missing signature');
        }
        const params = {
          chat_id: String(chatId),
          signature: state.signature,
          id: String(id || ''),
        }
        if (state.userId) {
          params.user_id = String(state.userId);
        }
        await apiDeleteJSON('/api/settings/memory', params);
      },
    },
    getters: {
      signature({ state }) {
        return state.signature;
      },
      userId({ state }) {
        return state.userId;
      },
      chats({ state }) {
        return state.chats;
      },
      lastSettings({ state }) {
        return state.lastSettings;
      },
    },
  });

  const app = new Framework7({
    el: '#app',
    name: 'Plotva Settings',
    theme: tgCtx.theme,
    darkMode: tgCtx.isDark,
    colors: {
      primary: getComputedStyle(document.documentElement).getPropertyValue('--tg-theme-button-color') || '#007aff',
    },
    store,
    view: {
      browserHistory: !tgCtx.isTelegram,
    },
    routes: [
      { path: '/', content: landingPageContent, on: { pageInit: landingPageInit } },
      { path: '/chats/', content: chatsPageContent, on: { pageInit: chatsPageInit } },
      { path: '/general/', content: settingsPageContent, on: { pageInit: (e, page) => settingsPageInit(e, page, 'general') } },
      { path: '/chat/:chatId/', content: settingsPageContent, on: { pageInit: (e, page) => settingsPageInit(e, page, 'chat') } },
      { path: '(.*)', content: notFoundPageContent },
    ],
  });

  if (tgCtx.tg.onEvent) {
    tgCtx.tg.onEvent('themeChanged', () => {
      const isDark = tgCtx.tg.colorScheme === 'dark';
      document.body.classList.toggle('theme-dark', isDark);
      if (typeof app.setDarkMode === 'function') {
        app.setDarkMode(isDark);
      }
    });
  }

  const mainView = app.views.create('.view-main', { url: '/', loadInitialPage: false });

  bootstrapInitialNavigation(mainView.router, {
    mode: modeFromQuery,
    chatId: chatIdFromQuery,
    browserHistory: !tgCtx.isTelegram,
  });

  if (!store.state.signature) {
    tgCtx.tg.showAlert('Ошибка: отсутствует подпись запроса');
  }

  function landingPageInit(_e, page) {
    const router = page.router;

    const warnEl = page.el.querySelector('#signatureWarning');
    if (warnEl) {
      warnEl.style.display = store.state.signature ? 'none' : '';
    }

    const chatsTile = page.el.querySelector('#tileChats');
    if (chatsTile) {
      chatsTile.addEventListener('click', (evt) => {
        evt.preventDefault();
        router?.navigate?.('/chats/');
      });
    }

    const userSettingsTile = page.el.querySelector('#tileUserSettings');
    if (userSettingsTile) {
      userSettingsTile.addEventListener('click', (evt) => {
        evt.preventDefault();
        router?.navigate?.('/general/');
      });
    }

    const titleEl = page.el.querySelector('.navbar .title');
    const debugBlock = page.el.querySelector('#debugBlock');
    const debugInfo = page.el.querySelector('#debugInfo');
    if (!titleEl || !debugBlock || !debugInfo) return;

    let tapCount = 0;
    let timer = null;
    titleEl.addEventListener('click', () => {
      tapCount += 1;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        tapCount = 0;
      }, 500);
      if (tapCount !== 3) return;
      tapCount = 0;
      debugBlock.style.display = debugBlock.style.display === 'none' ? '' : 'none';
      debugInfo.textContent = `User ID: ${store.state.userId || '—'} | Platform: ${tgCtx.tg.platform || '—'} | Version: ${tgCtx.tg.version || '—'}`;
    });
  }

  function chatsPageInit(_e, page) {
    const router = page.router;
    const backEl = page.el.querySelector('#backLink');
    if (backEl) {
      backEl.addEventListener('click', (evt) => {
        evt.preventDefault();
        if (router?.history && router.history.length > 1) {
          router.back();
          return;
        }
        router?.navigate?.('/');
      });
    }

    const listEl = page.el.querySelector('#chatList');
    const emptyEl = page.el.querySelector('#emptyBlock');
    if (!listEl) return;

    if (!store.state.signature) {
      tgCtx.tg.showAlert('Ошибка: отсутствует подпись запроса');
      return;
    }
    if (!store.state.userId) {
      tgCtx.tg.showAlert('Ошибка: не определен пользователь');
      return;
    }

    app.preloader.show();
    store
      .dispatch('loadChats')
      .then(() => {
        const chats = Array.isArray(store.state.chats) ? store.state.chats : [];
        listEl.innerHTML = '';
        if (chats.length === 0) {
          if (emptyEl) emptyEl.style.display = '';
          return;
        }
        if (emptyEl) emptyEl.style.display = 'none';

        chats.forEach((chat) => {
          const li = document.createElement('li');
          const a = document.createElement('a');
          a.className = 'item-link item-content';
          a.href = `/chat/${chat.id}/`;

          const inner = document.createElement('div');
          inner.className = 'item-inner';

          const title = document.createElement('div');
          title.className = 'item-title';
          title.textContent = chat.title || `Chat ${chat.id}`;

          const after = document.createElement('div');
          after.className = 'item-after';
          after.textContent = chatTypeLabel(chat.type);

          inner.appendChild(title);
          inner.appendChild(after);
          a.appendChild(inner);
          li.appendChild(a);
          listEl.appendChild(li);
        });
      })
      .catch((err) => {
        console.error('Error loading chats:', err);
        tgCtx.tg.showAlert('Ошибка загрузки списка чатов');
      })
      .finally(() => {
        app.preloader.hide();
      });
  }

  function settingsPageInit(_e, page, scope) {
    const router = page.router;
    const route = page.route;
    const formEl = page.el.querySelector('#settingsForm');
    const titleEl = page.el.querySelector('#pageTitle');
    const saveToolbarEl = page.el.querySelector('#saveToolbar');
    const saveButton = page.el.querySelector('#saveButton');
    const cancelButton = page.el.querySelector('#cancelButton');
    const backLink = page.el.querySelector('#backLink');
    const detailsEl = page.el.querySelector('#detailsSection');
    const groupSettingsEl = page.el.querySelector('#groupSettingsSection');
    const privateInfoEl = page.el.querySelector('#privateInfoSection');
    const disableRandomReactivityItem = page.el.querySelector('#disableRandomReactivityItem');
    const hideOriginalDrawPromptItem = page.el.querySelector('#hideOriginalDrawPromptItem');
    const hideOriginalDrawPromptToggle = page.el.querySelector('#hideOriginalDrawPromptToggle');
    const hideOriginalDrawPromptNotice = page.el.querySelector('#hideOriginalDrawPromptNotice');
    const deputiesSectionEl = page.el.querySelector('#deputiesSection');
    const deputiesOpenerEl = page.el.querySelector('#deputiesAutocompleteOpener');
    const deputiesSummaryEl = page.el.querySelector('#deputiesSummary');
    const deputiesCountEl = page.el.querySelector('#deputiesCount');
    const deputiesPreviewWrapEl = page.el.querySelector('#deputiesPreviewWrap');
    const deputiesPreviewEl = page.el.querySelector('#deputiesPreview');
    const memoryTitleEl = page.el.querySelector('#memoryTitle');
    const memoryListBlockEl = page.el.querySelector('#memoryListBlock');
    const memoryListEl = page.el.querySelector('#memoryList');
    const memoryEmptyEl = page.el.querySelector('#memoryEmpty');
    const personaItemEl = page.el.querySelector('#customPersonaItem');
    const personaTextareaEl = formEl?.querySelector('textarea[name="custom_persona"]');
    const personaCounterEl = page.el.querySelector('#personaCounter');
    const personaErrorEl = page.el.querySelector('#personaError');

    if (!formEl || !saveToolbarEl) return;

    const userId = store.state.userId;
    const routeChatId = parseIntID(route?.params?.chatId);
    const chatId = scope === 'general' ? userId : routeChatId;

    if (!store.state.signature) {
      tgCtx.tg.showAlert('Ошибка: отсутствует подпись запроса');
      router?.navigate?.('/');
      return;
    }
    if (!chatId) {
      tgCtx.tg.showAlert('Ошибка: не определен chat_id');
      router?.navigate?.('/');
      return;
    }

    const formId = scope === 'general' ? 'settings-general' : `settings-chat-${chatId}`;
    formEl.id = formId;

    if (scope !== 'general' && disableRandomReactivityItem) {
      disableRandomReactivityItem.remove();
    }
    if (scope !== 'general' && hideOriginalDrawPromptItem) {
      hideOriginalDrawPromptItem.remove();
    }
    if (scope === 'general') {
      formEl.querySelector('input[name="enable_global_text_reply"]')?.closest('li')?.remove();
      formEl.querySelector('input[name="enable_global_draw_reply"]')?.closest('li')?.remove();
      if (deputiesSectionEl) {
        deputiesSectionEl.remove();
      }
    }

    let chatType = null;
    let originalState = null;
    let dirty = false;
    let suppressFormEvents = true;
    let isVIPUser = false;
    let personaIsValid = true;
    let canManageDeputies = false;
    let selectedDeputies = [];
    let deputyAutocomplete = null;

    const isGroupChatType = (ct) => ct === 'group' || ct === 'supergroup';

    const moodAlignmentPickerInput = formEl.querySelector('#moodAlignmentPickerInput');
    const reactivityPickerInput = formEl.querySelector('#reactivityPickerInput');
    const dailyGameThemePickerInput = formEl.querySelector('#dailyGameThemePickerInput');

    const makeValueDisplayMap = (values, displayValues) => {
      const m = {};
      const n = Math.min(values?.length || 0, displayValues?.length || 0);
      for (let i = 0; i < n; i += 1) {
        m[String(values[i])] = String(displayValues[i]);
      }
      return m;
    };

    const moodAlignmentPickerValues = ['neutral', 'friendly', 'explicit_raw'];
    const moodAlignmentPickerDisplayValues = ['😐 Нейтральный', '😊 Дружелюбный', '😈 Дерзкий'];
    const moodAlignmentDisplayByValue = makeValueDisplayMap(moodAlignmentPickerValues, moodAlignmentPickerDisplayValues);

    const reactivityPickerValues = ['0', '1', '3'];
    const reactivityPickerDisplayValues = ['🔇 Выключена', '🔈 Низкая, 1%', '🔊 Средняя, 3%'];
    const reactivityDisplayByValue = makeValueDisplayMap(reactivityPickerValues, reactivityPickerDisplayValues);

    const dailyGameThemePickerValues = ['auto', 'king', 'pidor', 'kotik', 'lucky'];
    const dailyGameThemePickerDisplayValues = ['🧠 На выбор пользователя', '👑 Король горы', '🌈 Пидор дня', '🐾 Котик дня', '🏛️ Чиновник дня'];
    const dailyGameThemeDisplayByValue = makeValueDisplayMap(dailyGameThemePickerValues, dailyGameThemePickerDisplayValues);

    const dispatchFormChange = (name) => {
      const el = formEl.querySelector(`[name="${cssEscape(name)}"]`);
      if (!el) return;
      try {
        el.dispatchEvent(new Event('change', { bubbles: true }));
      } catch (_) { }
    };

    let moodAlignmentPicker = null;
    let reactivityPicker = null;
    let dailyGameThemePicker = null;

    if (moodAlignmentPickerInput) {
      moodAlignmentPicker = app.picker.create({
        inputEl: moodAlignmentPickerInput,
        openIn: 'sheet',
        toolbarCloseText: 'Готово',
        formatValue(values, displayValues) {
          const display = Array.isArray(displayValues) ? displayValues[0] : null;
          if (typeof display === 'string' && display !== '') return display;
          const value = Array.isArray(values) ? values[0] : '';
          return moodAlignmentDisplayByValue[String(value ?? '')] || String(value ?? '');
        },
        cols: [{ values: moodAlignmentPickerValues, displayValues: moodAlignmentPickerDisplayValues }],
        on: {
          change(_picker, values, displayValues) {
            if (suppressFormEvents) return;
            const v = Array.isArray(values) ? values[0] : null;
            const dv = Array.isArray(displayValues) ? displayValues[0] : null;
            if (typeof dv === 'string') {
              moodAlignmentPickerInput.value = dv;
            }
            if (v != null) {
              setValue(formEl, 'mood_alignment', v);
              dispatchFormChange('mood_alignment');
            }
          },
        },
      });
    }

    if (reactivityPickerInput) {
      reactivityPicker = app.picker.create({
        inputEl: reactivityPickerInput,
        openIn: 'sheet',
        toolbarCloseText: 'Готово',
        formatValue(values, displayValues) {
          const display = Array.isArray(displayValues) ? displayValues[0] : null;
          if (typeof display === 'string' && display !== '') return display;
          const value = Array.isArray(values) ? values[0] : '';
          return reactivityDisplayByValue[String(value ?? '')] || String(value ?? '');
        },
        cols: [{ values: reactivityPickerValues, displayValues: reactivityPickerDisplayValues }],
        on: {
          change(_picker, values, displayValues) {
            if (suppressFormEvents) return;
            const v = Array.isArray(values) ? values[0] : null;
            const dv = Array.isArray(displayValues) ? displayValues[0] : null;
            if (typeof dv === 'string') {
              reactivityPickerInput.value = dv;
            }
            if (v != null) {
              setValue(formEl, 'reactivity_percentage', v);
              dispatchFormChange('reactivity_percentage');
            }
          },
        },
      });
    }

    if (dailyGameThemePickerInput) {
      dailyGameThemePicker = app.picker.create({
        inputEl: dailyGameThemePickerInput,
        openIn: 'sheet',
        toolbarCloseText: 'Готово',
        formatValue(values, displayValues) {
          const display = Array.isArray(displayValues) ? displayValues[0] : null;
          if (typeof display === 'string' && display !== '') return display;
          const value = Array.isArray(values) ? values[0] : '';
          return dailyGameThemeDisplayByValue[String(value ?? '')] || String(value ?? '');
        },
        cols: [{ values: dailyGameThemePickerValues, displayValues: dailyGameThemePickerDisplayValues }],
        on: {
          change(_picker, values, displayValues) {
            if (suppressFormEvents) return;
            const v = Array.isArray(values) ? values[0] : null;
            const dv = Array.isArray(displayValues) ? displayValues[0] : null;
            if (typeof dv === 'string') {
              dailyGameThemePickerInput.value = dv;
            }
            if (v != null) {
              setValue(formEl, 'daily_game_theme', v);
              dispatchFormChange('daily_game_theme');
            }
          },
        },
      });
    }

    const syncPickerDisplays = () => {
      const moodAlignment = getValue(formEl, 'mood_alignment') || 'neutral';
      if (moodAlignmentPicker) {
        try {
          moodAlignmentPicker.setValue([moodAlignment]);
        } catch (_) { }
      }
      if (moodAlignmentPickerInput) {
        moodAlignmentPickerInput.value = moodAlignmentDisplayByValue[moodAlignment] || moodAlignment;
      }

      const reactivity = getValue(formEl, 'reactivity_percentage') || '3';
      if (reactivityPicker) {
        try {
          reactivityPicker.setValue([reactivity]);
        } catch (_) { }
      }
      if (reactivityPickerInput) {
        reactivityPickerInput.value = reactivityDisplayByValue[reactivity] || reactivity;
      }

      const dailyTheme = getValue(formEl, 'daily_game_theme') || 'auto';
      if (dailyGameThemePicker) {
        try {
          dailyGameThemePicker.setValue([dailyTheme]);
        } catch (_) { }
      }
      if (dailyGameThemePickerInput) {
        dailyGameThemePickerInput.value = dailyGameThemeDisplayByValue[dailyTheme] || dailyTheme;
      }
    };

    const setSaveButtonDisabled = (disabled) => {
      if (!saveButton) return;
      saveButton.classList.toggle('disabled', Boolean(disabled));
      saveButton.setAttribute('aria-disabled', disabled ? 'true' : 'false');
    };

    const syncSaveButtonState = () => {
      setSaveButtonDisabled(!dirty || !personaIsValid);
    };

    const setSavebarVisible = (visible) => {
      dirty = Boolean(visible);
      if (dirty) {
        saveToolbarEl.style.display = '';
        page.el.classList.add('settings-savebar-visible');
        tgCtx.tg.enableClosingConfirmation();
      } else {
        saveToolbarEl.style.display = 'none';
        page.el.classList.remove('settings-savebar-visible');
        tgCtx.tg.disableClosingConfirmation();
      }
      syncSaveButtonState();
    };

    const personaCharsLen = (value) => Array.from(String(value ?? '')).length;

    const updatePersonaState = () => {
      if (!personaTextareaEl) return true;
      const length = personaCharsLen(personaTextareaEl.value);
      const remaining = CUSTOM_PERSONA_MAX_CHARS - length;
      personaIsValid = length <= CUSTOM_PERSONA_MAX_CHARS;

      if (personaCounterEl) {
        if (remaining >= 0) {
          personaCounterEl.textContent = `Осталось ${remaining} символов`;
        } else {
          personaCounterEl.textContent = `Превышение на ${Math.abs(remaining)} символов`;
        }
      }

      if (personaItemEl) {
        personaItemEl.classList.toggle('item-input-invalid', !personaIsValid);
      }
      if (personaErrorEl) {
        personaErrorEl.textContent = personaIsValid ? '' : `Максимум ${CUSTOM_PERSONA_MAX_CHARS} символов`;
      }

      syncSaveButtonState();
      return personaIsValid;
    };

    const updateHideOriginalPromptAccess = () => {
      if (!hideOriginalDrawPromptItem) return;
      const hidePromptCheckbox = formEl.querySelector('input[name="hide_original_draw_prompt"]');
      if (!hidePromptCheckbox) return;
      if (isVIPUser) {
        hidePromptCheckbox.disabled = false;
        if (hideOriginalDrawPromptToggle) {
          hideOriginalDrawPromptToggle.classList.remove('disabled');
        }
        if (hideOriginalDrawPromptNotice) {
          hideOriginalDrawPromptNotice.textContent = 'Скрывает текст вашего запроса в подписи изображения.';
        }
        return;
      }
      hidePromptCheckbox.checked = false;
      hidePromptCheckbox.disabled = true;
      if (hideOriginalDrawPromptToggle) {
        hideOriginalDrawPromptToggle.classList.add('disabled');
      }
      if (hideOriginalDrawPromptNotice) {
        hideOriginalDrawPromptNotice.textContent = 'Доступно только VIP-подписчикам';
      }
    };

    const deputyStatusLabel = (status) => {
      switch (String(status || '').toLowerCase()) {
        case 'creator':
          return 'Владелец';
        case 'administrator':
          return 'Администратор';
        case 'member':
          return 'Участник';
        default:
          return '';
      }
    };

    const normalizeDeputyItems = (items) => {
      const seen = new Set();
      return (Array.isArray(items) ? items : [])
        .map((item) => {
          const id = parseIntID(item?.id);
          if (!id || seen.has(id)) return null;
          seen.add(id);
          const username = String(item?.username || '').replace(/^@+/, '').trim();
          const firstName = String(item?.first_name || '').trim();
          const lastName = String(item?.last_name || '').trim();
          const fallbackName = [firstName, lastName].filter(Boolean).join(' ');
          const displayName = String(item?.display_name || fallbackName || (username ? `@${username}` : `User ${id}`)).trim();
          return {
            id,
            first_name: firstName,
            last_name: lastName,
            username,
            status: String(item?.status || '').trim(),
            display_name: displayName,
          };
        })
        .filter(Boolean)
        .sort((a, b) => {
          if (a.display_name === b.display_name) return a.id - b.id;
          return a.display_name.localeCompare(b.display_name, 'ru');
        });
    };

    const cloneDeputyItems = (items) => normalizeDeputyItems(items).map((item) => ({ ...item }));

    const deputySummaryText = (items) => {
      if (!items.length) {
        return 'Выберите участников чата, которые смогут управлять настройками Плотвы.';
      }
      if (items.length === 1) {
        return items[0].display_name;
      }
      const preview = items.slice(0, 2).map((item) => item.display_name).join(', ');
      if (items.length === 2) return preview;
      return `${preview} +${items.length - 2}`;
    };

    const syncDeputyPreview = () => {
      if (!deputiesSummaryEl || !deputiesCountEl || !deputiesPreviewWrapEl || !deputiesPreviewEl) return;
      const deputies = cloneDeputyItems(selectedDeputies);
      deputiesSummaryEl.textContent = deputySummaryText(deputies);
      deputiesCountEl.textContent = deputies.length ? String(deputies.length) : '0';
      deputiesPreviewEl.innerHTML = '';
      if (!deputies.length) {
        deputiesPreviewWrapEl.style.display = 'none';
        return;
      }
      deputies.forEach((deputy) => {
        const chip = document.createElement('div');
        chip.className = 'chip chip-outline';
        const label = document.createElement('div');
        label.className = 'chip-label';
        label.textContent = deputy.display_name;
        chip.appendChild(label);
        deputiesPreviewEl.appendChild(chip);
      });
      deputiesPreviewWrapEl.style.display = '';
    };

    const updateDeputyAutocompleteValue = () => {
      if (!deputyAutocomplete) return;
      deputyAutocomplete.value = cloneDeputyItems(selectedDeputies);
      if (deputyAutocomplete.opened) {
        deputyAutocomplete.updateValues();
        if (typeof deputyAutocomplete.source === 'function') {
          deputyAutocomplete.source(deputyAutocomplete.searchbar?.value || '');
        }
      }
    };

    const initDeputyAutocomplete = () => {
      if (!deputiesOpenerEl || deputyAutocomplete || !canManageDeputies) return;
      deputyAutocomplete = app.autocomplete.create({
        openIn: 'popup',
        openerEl: deputiesOpenerEl,
        multiple: true,
        autoFocus: true,
        preloader: true,
        requestSourceOnOpen: true,
        value: cloneDeputyItems(selectedDeputies),
        valueProperty: 'id',
        textProperty: 'display_name',
        searchbarPlaceholder: 'Имя или @username',
        searchbarDisableText: 'Отмена',
        popupCloseLinkText: 'Готово',
        pageTitle: 'Заместители',
        notFoundText: 'Ничего не найдено',
        source(query, render) {
          const autocomplete = this;
          autocomplete.preloaderShow();
          store
            .dispatch('loadDeputyCandidates', {
              chatId,
              query: query || '',
              limit: 50,
            })
            .then((items) => {
              render(normalizeDeputyItems(items));
            })
            .catch((err) => {
              console.error('Error loading deputy candidates:', err);
              render([]);
            })
            .finally(() => {
              autocomplete.preloaderHide();
            });
        },
        renderItem(item, index) {
          const sourceItem = this.items?.[index] || this.value?.[index] || {};
          const meta = [];
          if (sourceItem.username) {
            meta.push(`@${escapeHTML(sourceItem.username)}`);
          }
          const statusLabel = deputyStatusLabel(sourceItem.status);
          if (statusLabel) {
            meta.push(escapeHTML(statusLabel));
          }
          return `
            <li>
              <label class="item-${item.inputType} item-content">
                <input type="${item.inputType}" name="${item.inputName}" value="${escapeAttr(item.value)}" ${item.selected ? 'checked' : ''}>
                <i class="icon icon-${item.inputType}"></i>
                <div class="item-inner">
                  <div class="item-title">${item.text}</div>
                  ${meta.length ? `<div class="item-text autocomplete-deputy-meta">${meta.join(' • ')}</div>` : ''}
                </div>
              </label>
            </li>
          `;
        },
        on: {
          change(value) {
            selectedDeputies = normalizeDeputyItems(value);
            syncDeputyPreview();
            if (!suppressFormEvents) {
              updateDirty();
              persistDraft();
            }
          },
        },
      });
    };

    const settingsStateSnapshot = (state) => ({
      enable_global_text_reply: state.enable_global_text_reply,
      enable_global_draw_reply: state.enable_global_draw_reply,
      disable_random_reactivity: state.disable_random_reactivity,
      hide_original_draw_prompt: state.hide_original_draw_prompt,
      mood_alignment: state.mood_alignment,
      custom_persona: state.custom_persona,
      enable_profanity: state.enable_profanity,
      enable_obscenifier: state.enable_obscenifier,
      enable_greet_joiners: state.enable_greet_joiners,
      reactivity_percentage: state.reactivity_percentage,
      enable_daily_game: state.enable_daily_game,
      daily_game_theme: state.daily_game_theme,
    });

    const currentState = () => {
      return {
        enable_global_text_reply: getCheckbox(formEl, 'enable_global_text_reply') ?? true,
        enable_global_draw_reply: getCheckbox(formEl, 'enable_global_draw_reply') ?? true,
        disable_random_reactivity: getCheckbox(formEl, 'disable_random_reactivity') ?? false,
        hide_original_draw_prompt: getCheckbox(formEl, 'hide_original_draw_prompt') ?? false,
        mood_alignment: getValue(formEl, 'mood_alignment') || 'neutral',
        custom_persona: getValue(formEl, 'custom_persona') || '',
        enable_profanity: getCheckbox(formEl, 'enable_profanity') ?? false,
        enable_obscenifier: getCheckbox(formEl, 'enable_obscenifier') ?? false,
        enable_greet_joiners: getCheckbox(formEl, 'enable_greet_joiners') ?? false,
        reactivity_percentage: getValue(formEl, 'reactivity_percentage') || '3',
        enable_daily_game: getCheckbox(formEl, 'enable_daily_game') ?? true,
        daily_game_theme: getValue(formEl, 'daily_game_theme') || 'auto',
        deputies: cloneDeputyItems(selectedDeputies),
      };
    };

    const writeState = (s) => {
      setCheckbox(formEl, 'enable_global_text_reply', s.enable_global_text_reply);
      setCheckbox(formEl, 'enable_global_draw_reply', s.enable_global_draw_reply);
      setCheckbox(formEl, 'disable_random_reactivity', s.disable_random_reactivity);
      setCheckbox(formEl, 'hide_original_draw_prompt', s.hide_original_draw_prompt);
      setValue(formEl, 'mood_alignment', s.mood_alignment);
      setValue(formEl, 'custom_persona', s.custom_persona);
      setCheckbox(formEl, 'enable_profanity', s.enable_profanity);
      setCheckbox(formEl, 'enable_obscenifier', s.enable_obscenifier);
      setCheckbox(formEl, 'enable_greet_joiners', s.enable_greet_joiners);
      setValue(formEl, 'reactivity_percentage', s.reactivity_percentage);
      setCheckbox(formEl, 'enable_daily_game', s.enable_daily_game);
      setValue(formEl, 'daily_game_theme', s.daily_game_theme);
      selectedDeputies = cloneDeputyItems(s.deputies);
      syncPickerDisplays();
      updatePersonaState();
      updateHideOriginalPromptAccess();
      syncDeputyPreview();
      updateDeputyAutocompleteValue();
    };

    const updateVisibility = () => {
      const enabled = getCheckbox(formEl, 'enable_global_text_reply') ?? true;
      const isGroup = isGroupChatType(chatType);
      if (detailsEl) detailsEl.style.display = enabled ? '' : 'none';
      if (groupSettingsEl) groupSettingsEl.style.display = enabled && isGroup ? '' : 'none';
      if (deputiesSectionEl) deputiesSectionEl.style.display = canManageDeputies && isGroup ? '' : 'none';
      if (privateInfoEl) privateInfoEl.style.display = enabled && !isGroup ? '' : 'none';
    };

    const updateDirty = () => {
      if (!originalState) return;
      const now = currentState();
      const nowDirty = JSON.stringify(now) !== JSON.stringify(originalState);
      setSavebarVisible(nowDirty);
    };

    const persistDraft = () => {
      if (!originalState) return;
      const now = currentState();
      if (JSON.stringify(now) === JSON.stringify(originalState)) {
        app.form.removeFormData(formId);
        return;
      }
      app.form.storeFormData(formId, { ...now, _ts: Date.now() });
    };

    const restoreDraftIfAny = () => {
      const draft = app.form.getFormData(formId);
      if (!draft || typeof draft !== 'object' || Object.keys(draft).length === 0) return;
      app.dialog.confirm('Найдены несохранённые изменения. Восстановить?', () => {
        suppressFormEvents = true;
        writeState(draft);
        suppressFormEvents = false;
        updateVisibility();
        setSavebarVisible(true);
        updateDirty();
      }, () => {
        app.form.removeFormData(formId);
      });
    };

    const setMemoryVisible = (hasItems) => {
      if (memoryListBlockEl) memoryListBlockEl.style.display = hasItems ? '' : 'none';
      if (memoryEmptyEl) memoryEmptyEl.style.display = hasItems ? 'none' : '';
      if (memoryTitleEl) memoryTitleEl.style.display = '';
    };

    const memoryScopeBadge = (visibility) => {
      if (scope !== 'general') return null;
      if (visibility === 'private_chat' || visibility === 'chat_user') {
        return { text: 'Приватный чат', className: 'memory-scope-private' };
      }
      if (visibility === 'public_user') {
        return { text: 'Публичные группы', className: 'memory-scope-public' };
      }
      return null;
    };

    const renderMemory = (items) => {
      if (!memoryListEl) return;
      const list = Array.isArray(items) ? items : [];
      memoryListEl.innerHTML = '';
      if (list.length === 0) {
        setMemoryVisible(false);
        return;
      }
      setMemoryVisible(true);
      list.forEach((card) => {
        const id = Number(card?.id || 0);
        const titleText = String(card?.card_type || 'memory').trim();
        const value = String(card?.fact_text || '').trim();
        const visibility = String(card?.visibility || '').trim().toLowerCase();
        const li = document.createElement('li');
        li.className = 'swipeout';
        const content = document.createElement('div');
        content.className = 'swipeout-content item-content';
        const inner = document.createElement('div');
        inner.className = 'item-inner';
        const titleRow = document.createElement('div');
        titleRow.className = 'item-title-row';
        const title = document.createElement('div');
        title.className = 'item-title';
        title.textContent = titleText || 'Карточка';
        titleRow.appendChild(title);
        const badgeMeta = memoryScopeBadge(visibility);
        if (badgeMeta) {
          const badge = document.createElement('span');
          badge.className = `memory-scope-badge ${badgeMeta.className}`;
          badge.textContent = badgeMeta.text;
          titleRow.appendChild(badge);
        }
        inner.appendChild(titleRow);
        if (value) {
          const text = document.createElement('div');
          text.className = 'item-text';
          text.textContent = value;
          inner.appendChild(text);
        }
        content.appendChild(inner);
        li.appendChild(content);

        const actions = document.createElement('div');
        actions.className = 'swipeout-actions-right';
        const removeLink = document.createElement('a');
        removeLink.href = '#';
        removeLink.className = 'color-red';
        removeLink.textContent = 'Удалить';
        removeLink.addEventListener('click', (evt) => {
          evt.preventDefault();
          if (!id) return;
          const label = value || titleText || 'карточку памяти';
          app.dialog.confirm(`Удалить карточку памяти «${label}»?`, () => {
            app.preloader.show();
            store
              .dispatch('deleteMemory', { chatId, id })
              .then(() => {
                showToast('Карточка удалена');
                if (app.swipeout && typeof app.swipeout.close === 'function') {
                  app.swipeout.close(li);
                }
                loadMemory();
              })
              .catch((err) => {
                console.error('Error deleting memory:', err);
                tgCtx.tg.showAlert('Ошибка удаления карточки памяти');
              })
              .finally(() => {
                app.preloader.hide();
              });
          });
        });
        actions.appendChild(removeLink);
        li.appendChild(actions);
        memoryListEl.appendChild(li);
      });
    };

    const loadMemory = () => {
      if (!memoryListEl) return;
      store
        .dispatch('loadMemory', { chatId, limit: 200 })
        .then((res) => {
          renderMemory(res?.cards);
        })
        .catch((err) => {
          console.error('Error loading memory:', err);
          tgCtx.tg.showAlert('Ошибка загрузки памяти');
          setMemoryVisible(false);
        });
    };

    const navigateBack = () => {
      if (router?.history && router.history.length > 1) {
        router.back();
        return;
      }
      router?.navigate?.('/');
    };

    if (backLink) {
      backLink.addEventListener('click', (evt) => {
        evt.preventDefault();
        if (!dirty) {
          navigateBack();
          return;
        }
        app.dialog.confirm('Есть несохранённые изменения. Выйти без сохранения?', () => {
          app.form.removeFormData(formId);
          tgCtx.tg.disableClosingConfirmation();
          navigateBack();
        });
      });
    }

    app.preloader.show();
    store
      .dispatch('loadSettings', { chatId })
      .then((settings) => {
        chatType = settings?.chat_type || null;
        isVIPUser = settings?.is_vip === true;
        canManageDeputies = scope === 'chat' && settings?.can_manage_deputies === true;
        if (titleEl) {
          titleEl.textContent = scope === 'general' ? 'Настройки пользователя' : (settings?.chat_title || 'Настройки чата');
        }
        const stateFromServer = {
          enable_global_text_reply: scope === 'general' ? true : settings?.enable_global_text_reply !== false,
          enable_global_draw_reply: scope === 'general' ? true : settings?.enable_global_draw_reply === true,
          disable_random_reactivity: scope === 'general' && settings?.disable_random_reactivity === true,
          hide_original_draw_prompt: scope === 'general' && isVIPUser && settings?.hide_original_draw_prompt === true,
          mood_alignment: settings?.mood_alignment || 'neutral',
          custom_persona: settings?.custom_persona || '',
          enable_profanity: settings?.enable_profanity === true,
          enable_obscenifier: settings?.enable_obscenifier === true,
          enable_greet_joiners: settings?.enable_greet_joiners === true,
          reactivity_percentage: String(settings?.reactivity_percentage ?? 3),
          enable_daily_game: settings?.enable_daily_game !== false,
          daily_game_theme: settings?.daily_game_theme || 'auto',
          deputies: canManageDeputies ? normalizeDeputyItems(settings?.deputies) : [],
        };
        originalState = stateFromServer;
        suppressFormEvents = true;
        writeState(stateFromServer);
        suppressFormEvents = false;
        if (canManageDeputies) {
          initDeputyAutocomplete();
        }
        updateVisibility();
        setSavebarVisible(false);
        restoreDraftIfAny();
        loadMemory();
      })
      .catch((err) => {
        console.error('Error loading settings:', err);
        tgCtx.tg.showAlert('Ошибка загрузки настроек');
        router?.navigate?.('/');
      })
      .finally(() => {
        app.preloader.hide();
      });

    const onChange = () => {
      if (suppressFormEvents) return;
      updatePersonaState();
      updateVisibility();
      updateDirty();
      persistDraft();
    };
    formEl.addEventListener('input', onChange);
    formEl.addEventListener('change', onChange);

    if (saveButton) {
      saveButton.addEventListener('click', (evt) => {
        evt.preventDefault();
        if (!dirty) return;
        if (!updatePersonaState()) return;
        const s = currentState();
        const isGroup = isGroupChatType(chatType);
        const settingsChanged = JSON.stringify(settingsStateSnapshot(s)) !== JSON.stringify(settingsStateSnapshot(originalState || {}));
        const deputiesChanged = JSON.stringify(s.deputies) !== JSON.stringify(originalState?.deputies || []);

        const payload = {
          chat_id: Number(chatId),
          user_id: store.state.userId ? Number(store.state.userId) : undefined,
          signature: store.state.signature,
          mood_alignment: s.mood_alignment,
          custom_persona: s.custom_persona,
          reactivity_percentage: isGroup ? Number(s.reactivity_percentage) : 100,
          enable_greet_joiners: s.enable_greet_joiners,
          enable_profanity: s.enable_profanity,
          enable_obscenifier: s.enable_obscenifier,
        };
        if (scope !== 'general') {
          payload.enable_global_text_reply = s.enable_global_text_reply;
          payload.enable_global_draw_reply = s.enable_global_draw_reply;
        }
        if (scope === 'general') {
          payload.disable_random_reactivity = s.disable_random_reactivity;
          payload.hide_original_draw_prompt = isVIPUser && s.hide_original_draw_prompt;
        }

        if (isGroup) {
          payload.enable_daily_game = s.enable_daily_game;
          payload.daily_game_theme = s.daily_game_theme;
        }

        app.preloader.show();
        let saveFlow = Promise.resolve();
        if (settingsChanged) {
          saveFlow = saveFlow.then(() => store.dispatch('saveSettings', payload));
        }
        if (canManageDeputies && deputiesChanged) {
          saveFlow = saveFlow
            .then(() => store.dispatch('saveDeputies', {
              chat_id: Number(chatId),
              user_id: store.state.userId ? Number(store.state.userId) : undefined,
              signature: store.state.signature,
              deputy_ids: s.deputies.map((item) => Number(item.id)),
            }))
            .then((res) => {
              selectedDeputies = normalizeDeputyItems(res?.deputies || s.deputies);
              syncDeputyPreview();
              updateDeputyAutocompleteValue();
            });
        }
        saveFlow
          .then(() => {
            showToast('Настройки сохранены');
            originalState = currentState();
            setSavebarVisible(false);
            app.form.removeFormData(formId);
          })
          .catch((err) => {
            console.error('Error saving settings:', err);
            tgCtx.tg.showAlert('Ошибка сохранения настроек');
          })
          .finally(() => {
            app.preloader.hide();
          });
      });
    }

    if (cancelButton) {
      cancelButton.addEventListener('click', (evt) => {
        evt.preventDefault();
        if (!dirty) return;
        app.dialog.confirm('Отменить все изменения?', () => {
          suppressFormEvents = true;
          writeState(originalState);
          suppressFormEvents = false;
          updateVisibility();
          setSavebarVisible(false);
          app.form.removeFormData(formId);
          showToast('Изменения отменены');
        });
      });
    }
  }

  function chatTypeLabel(type) {
    const labels = {
      private: '👤',
      group: '👥',
      supergroup: '👥',
      channel: '📢',
    };
    return labels[type] || '';
  }

  function bootstrapInitialNavigation(router, { mode, chatId, browserHistory }) {
    if (browserHistory && window.location.hash && window.location.hash.startsWith('#!')) {
      const url = window.location.hash.slice(2) || '/';
      router.navigate(url.startsWith('/') ? url : `/${url}`, { animate: false });
      return;
    }
    if (chatId) {
      router.navigate(`/chat/${chatId}/`, { animate: false });
      return;
    }
    if (mode === 'chat_selector') {
      router.navigate('/chats/', { animate: false });
      return;
    }
    if (mode === 'general') {
      router.navigate('/general/', { animate: false });
      return;
    }
    router.navigate('/', { animate: false });
  }

  function initTelegram() {
    try {
      const tg = window.Telegram.WebApp;
      tg.ready();
      tg.expand();
      tg.setHeaderColor(tg.headerColor || 'bg_color');
      tg.setBackgroundColor(tg.backgroundColor || 'bg_color');

      const user = tg.initDataUnsafe?.user || null;
      const userId = user?.id ?? null;
      const initData = typeof tg.initData === 'string' ? tg.initData : '';
      const isDark = tg.colorScheme === 'dark';
      document.body.classList.toggle('theme-dark', isDark);

      return {
        tg,
        userId,
        initData,
        isDark,
        theme: tg.platform === 'ios' || tg.platform === 'macos' ? 'ios' : 'auto',
        isTelegram: true,
      };
    } catch (e) {
      console.error('Failed to initialize Telegram WebApp:', e);
      const tg = {
        showAlert: (text) => alert(text),
        expand: () => { },
        setHeaderColor: () => { },
        setBackgroundColor: () => { },
        enableClosingConfirmation: () => { },
        disableClosingConfirmation: () => { },
        onEvent: null,
        colorScheme: 'light',
        platform: 'web',
        version: 'unknown',
        ready: () => { },
      };
      return {
        tg,
        userId: null,
        initData: '',
        isDark: false,
        theme: 'auto',
        isTelegram: false,
      };
    }
  }

  function parseIntID(value) {
    const v = String(value || '').trim();
    if (!v) return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    if (!Number.isInteger(n)) return null;
    return n;
  }

  async function apiGetJSON(path, params) {
    const url = new URL(path, window.location.origin);
    for (const [k, v] of Object.entries(params || {})) {
      if (v === undefined || v === null || String(v) === '') continue;
      url.searchParams.set(k, String(v));
    }
    return apiRequestJSON(url.toString(), { method: 'GET' });
  }

  async function apiDeleteJSON(path, params) {
    const url = new URL(path, window.location.origin);
    for (const [k, v] of Object.entries(params || {})) {
      if (v === undefined || v === null || String(v) === '') continue;
      url.searchParams.set(k, String(v));
    }
    return apiRequestJSON(url.toString(), { method: 'DELETE' });
  }

  async function apiRequestJSON(url, { method, body }) {
    const headers = { 'Content-Type': 'application/json' };
    if (tgCtx.initData) {
      headers['X-Telegram-Init-Data'] = tgCtx.initData;
    }
    const res = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });
    const raw = await res.text();
    if (!res.ok) {
      let serverError = raw;
      try {
        serverError = JSON.parse(raw)?.error || raw;
      } catch (_) { }
      throw new Error(serverError || `HTTP ${res.status}`);
    }
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch (_) {
      throw new Error('Invalid JSON response');
    }
  }

  function showToast(text) {
    app.toast
      .create({
        text,
        position: 'center',
        closeTimeout: 2000,
      })
      .open();
  }

  function getCheckbox(root, name) {
    const el = root.querySelector(`input[type="checkbox"][name="${cssEscape(name)}"]`);
    if (!el) return null;
    return Boolean(el.checked);
  }

  function setCheckbox(root, name, checked) {
    const el = root.querySelector(`input[type="checkbox"][name="${cssEscape(name)}"]`);
    if (!el) return;
    el.checked = Boolean(checked);
  }

  function getValue(root, name) {
    const el = root.querySelector(`[name="${cssEscape(name)}"]`);
    if (!el) return '';
    return String(el.value ?? '');
  }

  function setValue(root, name, value) {
    const el = root.querySelector(`[name="${cssEscape(name)}"]`);
    if (!el) return;
    el.value = value == null ? '' : String(value);
  }

  function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === 'function') {
      return window.CSS.escape(value);
    }
    return String(value).replace(/"/g, '\\"');
  }

  function escapeHTML(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function escapeAttr(value) {
    return escapeHTML(value);
  }
})();
