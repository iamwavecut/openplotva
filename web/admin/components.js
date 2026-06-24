/*
 * Plotva Admin — Component Library (pl-* custom elements + PL runtime)
 *
 * No build step, no dependencies. Loaded as a classic <script> in <head> so every pl-*
 * element upgrades on parse and the PL runtime is ready before the page's inline script runs.
 *
 * Design:
 *   - Light DOM custom elements (no Shadow DOM) so admin.css/tokens cascade and Chart.js sizing work.
 *   - Interactive controls (button, input, select, textarea, toggle, table) are pl-* elements.
 *   - Static surfaces (card, badge, skeleton, empty/error states) are CSS classes built via PL helpers.
 *   - Actions are wired by event delegation on [data-action] — no inline onclick anywhere.
 *   - Feedback is non-blocking: PL.toast. Dialogs are accessible: PL.alert / PL.confirm (focus-trapped).
 */
(function () {
    'use strict';

    /* ---------- safe text helper (kills the manual escapeHtml bug class) ---------- */
    function text(value) {
        return value === null || value === undefined ? '' : String(value);
    }

    /* Build an element with props + children (children are text nodes unless Node passed). */
    function el(tag, props, children) {
        const node = document.createElement(tag);
        if (props) {
            for (const key in props) {
                if (key === 'class') node.className = props[key];
                else if (key === 'dataset') Object.assign(node.dataset, props[key]);
                else if (key in node && key !== 'list') {
                    try { node[key] = props[key]; } catch (_) { node.setAttribute(key, props[key]); }
                } else node.setAttribute(key, props[key]);
            }
        }
        if (children != null) {
            (Array.isArray(children) ? children : [children]).forEach(function (c) {
                if (c == null) return;
                node.appendChild(c instanceof Node ? c : document.createTextNode(text(c)));
            });
        }
        return node;
    }

    /* ============================================================
       pl-button — host is the button (role + keyboard + states)
       ============================================================ */
    class PlButton extends HTMLElement {
        connectedCallback() {
            if (!this.hasAttribute('role')) this.setAttribute('role', 'button');
            if (!this.hasAttribute('tabindex')) this.setAttribute('tabindex', this.hasAttribute('disabled') ? '-1' : '0');
            this.addEventListener('keydown', this._onKey);
            this.addEventListener('click', this._onClick, true);
        }
        disconnectedCallback() {
            this.removeEventListener('keydown', this._onKey);
            this.removeEventListener('click', this._onClick, true);
        }
        _onKey(e) {
            if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar') {
                e.preventDefault();
                this.click();
            }
        }
        _onClick(e) {
            if (this.disabled || this.loading) {
                e.stopImmediatePropagation();
                e.preventDefault();
            }
        }
        get disabled() { return this.hasAttribute('disabled'); }
        set disabled(v) {
            if (v) { this.setAttribute('disabled', ''); this.setAttribute('aria-disabled', 'true'); this.setAttribute('tabindex', '-1'); }
            else { this.removeAttribute('disabled'); this.removeAttribute('aria-disabled'); this.setAttribute('tabindex', '0'); }
        }
        get loading() { return this.hasAttribute('loading'); }
        set loading(v) {
            if (v) { this.setAttribute('loading', ''); this.setAttribute('aria-busy', 'true'); }
            else { this.removeAttribute('loading'); this.removeAttribute('aria-busy'); }
        }
    }

    class PlButtonGroup extends HTMLElement {
        connectedCallback() { if (!this.hasAttribute('role')) this.setAttribute('role', 'group'); }
    }

    /* ============================================================
       Form controls — host wraps a native control that keeps the id
       so existing getElementById(id).value keeps working unchanged.
       ============================================================ */
    const MIRROR_ATTRS = ['id', 'name', 'type', 'placeholder', 'value', 'min', 'max', 'step',
        'maxlength', 'rows', 'cols', 'pattern', 'inputmode', 'autocomplete', 'readonly', 'required'];

    function mirrorAttrs(host, control) {
        MIRROR_ATTRS.forEach(function (a) {
            if (host.hasAttribute(a)) {
                control.setAttribute(a, host.getAttribute(a));
                if (a !== 'id') host.removeAttribute(a); // id moves to the control; others are duplicated harmlessly
            }
        });
        if (host.hasAttribute('id')) host.removeAttribute('id'); // id lives on the control only
        if (host.hasAttribute('disabled')) control.disabled = true;
    }

    function defineValueProxy(proto) {
        Object.defineProperty(proto, 'value', {
            get() { return this._control ? this._control.value : ''; },
            set(v) { if (this._control) this._control.value = v; },
            configurable: true
        });
    }

    class PlInput extends HTMLElement {
        connectedCallback() {
            if (this._control) return;
            this._control = el('input', { class: 'pl-field' });
            mirrorAttrs(this, this._control);
            if (!this._control.getAttribute('type')) this._control.type = 'text';
            this._control.addEventListener('input', () => this.dispatchEvent(new CustomEvent('pl:input', { bubbles: true })));
            this._control.addEventListener('change', () => this.dispatchEvent(new CustomEvent('pl:change', { bubbles: true })));
            this._control.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this.dispatchEvent(new CustomEvent('pl:enter', { bubbles: true }));
            });
            this.appendChild(this._control);
        }
        focus() { this._control && this._control.focus(); }
    }
    defineValueProxy(PlInput.prototype);

    class PlTextarea extends HTMLElement {
        connectedCallback() {
            if (this._control) return;
            this._control = el('textarea', { class: 'pl-field' });
            mirrorAttrs(this, this._control);
            const initial = this.getAttribute('text');
            if (initial != null) this._control.value = initial;
            this._control.addEventListener('input', () => this.dispatchEvent(new CustomEvent('pl:input', { bubbles: true })));
            this.appendChild(this._control);
        }
    }
    defineValueProxy(PlTextarea.prototype);

    class PlSelect extends HTMLElement {
        connectedCallback() {
            if (this._control) return;
            this._control = el('select', { class: 'pl-field' });
            mirrorAttrs(this, this._control);
            // move declarative <option> children into the native select
            Array.from(this.querySelectorAll('option')).forEach((opt) => this._control.appendChild(opt));
            if (this.hasAttribute('value')) this._control.value = this.getAttribute('value');
            this._control.addEventListener('change', () => this.dispatchEvent(new CustomEvent('pl:change', { bubbles: true })));
            this.appendChild(this._control);
        }
    }
    defineValueProxy(PlSelect.prototype);

    /* pl-field-group — label + control + help/error, wires aria automatically */
    let fieldSeq = 0;
    class PlFieldGroup extends HTMLElement {
        connectedCallback() {
            if (this._built) return;
            this._built = true;
            const labelText = this.getAttribute('label');
            const help = this.getAttribute('help');
            const required = this.hasAttribute('required');
            const control = this.querySelector('pl-input, pl-select, pl-textarea, input, select, textarea');
            let controlId = control && (control.querySelector ? (control.querySelector('.pl-field') || {}).id : control.id);
            if (control && !controlId) { controlId = 'pl-f' + (++fieldSeq); const inner = control.querySelector ? control.querySelector('.pl-field') : control; if (inner) inner.id = controlId; }
            if (labelText) {
                const label = el('label', { class: 'pl-field-label' });
                if (controlId) label.setAttribute('for', controlId);
                label.appendChild(document.createTextNode(labelText));
                if (required) label.appendChild(el('span', { class: 'pl-required', 'aria-hidden': 'true' }, '*'));
                this.insertBefore(label, this.firstChild);
            }
            if (help) this.appendChild(el('div', { class: 'pl-field-help' }, help));
            this._errorEl = el('div', { class: 'pl-field-error', role: 'alert' });
            this.appendChild(this._errorEl);
        }
        setError(message) {
            if (message) { this.setAttribute('invalid', ''); this._errorEl && (this._errorEl.textContent = message); }
            else { this.removeAttribute('invalid'); this._errorEl && (this._errorEl.textContent = ''); }
        }
    }

    /* ============================================================
       pl-toggle — accessible switch
       ============================================================ */
    class PlToggle extends HTMLElement {
        connectedCallback() {
            if (this._built) return;
            this._built = true;
            this.setAttribute('role', 'switch');
            this.setAttribute('aria-checked', this.hasAttribute('checked') ? 'true' : 'false');
            if (!this.hasAttribute('tabindex')) this.setAttribute('tabindex', this.hasAttribute('disabled') ? '-1' : '0');
            const label = this.getAttribute('label');
            this._track = el('span', { class: 'pl-toggle-track', 'aria-hidden': 'true' }, el('span', { class: 'pl-toggle-thumb' }));
            this.insertBefore(this._track, this.firstChild);
            if (label) this.appendChild(el('span', { class: 'pl-toggle-text' }, label));
            this.addEventListener('click', () => this.toggle());
            this.addEventListener('keydown', (e) => {
                if (e.key === ' ' || e.key === 'Enter') { e.preventDefault(); this.toggle(); }
            });
        }
        toggle() {
            if (this.hasAttribute('disabled')) return;
            this.checked = !this.checked;
            this.dispatchEvent(new CustomEvent('pl:change', { bubbles: true, detail: { checked: this.checked } }));
        }
        get checked() { return this.hasAttribute('checked'); }
        set checked(v) {
            if (v) this.setAttribute('checked', ''); else this.removeAttribute('checked');
            this.setAttribute('aria-checked', v ? 'true' : 'false');
        }
    }

    /* ============================================================
       pl-table — columns/rows props + idle/loading/empty/error states
       columns: [{ key, label, sortable, mono, num, render(row) -> string|Node, className }]
       ============================================================ */
    class PlTable extends HTMLElement {
        connectedCallback() {
            this._upgradeProp('columns');
            this._upgradeProp('rows');
            if (!this._columns) this._columns = [];
            if (!this._rows) this._rows = null;
            this._render();
        }
        _upgradeProp(name) {
            if (Object.prototype.hasOwnProperty.call(this, name)) {
                const v = this[name];
                delete this[name];
                this['_' + name] = v;
            }
        }
        set columns(c) { this._columns = c || []; this._render(); }
        get columns() { return this._columns || []; }
        set rows(r) { this._rows = r; this._state = null; this._render(); }
        get rows() { return this._rows || []; }
        set state(s) { this._state = s; this._render(); }
        get state() { return this._state; }
        set emptyTitle(v) { this._emptyTitle = v; }
        set emptyDesc(v) { this._emptyDesc = v; }
        set onRetry(fn) { this._onRetry = fn; }

        _sortBy(key) {
            const col = this._columns.find((c) => c.key === key);
            if (!col || !col.sortable) return;
            const dir = this._sortKey === key && this._sortDir === 'asc' ? 'desc' : 'asc';
            this._sortKey = key; this._sortDir = dir;
            this.dispatchEvent(new CustomEvent('pl:sort', { bubbles: true, detail: { key: key, dir: dir } }));
            if (this._rows) {
                this._rows = this._rows.slice().sort((a, b) => {
                    const va = a[key], vb = b[key];
                    if (va == null) return 1; if (vb == null) return -1;
                    return (va > vb ? 1 : va < vb ? -1 : 0) * (dir === 'asc' ? 1 : -1);
                });
            }
            this._render();
        }

        _render() {
            const cols = this._columns || [];
            this.textContent = '';
            const colCount = Math.max(cols.length, 1);

            if (this._state === 'loading') {
                this.appendChild(PL.skeletonTable(colCount, Number(this.getAttribute('skeleton-rows')) || 5, cols));
                return;
            }
            if (this._state === 'error') {
                this.appendChild(PL.errorStateNode({ message: this._errorMsg || 'Could not load data.', onRetry: this._onRetry }));
                return;
            }
            const table = el('table');
            if (cols.length) {
                const thead = el('thead');
                const tr = el('tr');
                cols.forEach((c) => {
                    const th = el('th', null, c.label != null ? c.label : c.key);
                    if (c.sortable) {
                        th.className = 'sortable';
                        th.tabIndex = 0;
                        if (this._sortKey === c.key) th.setAttribute('aria-sort', this._sortDir === 'asc' ? 'ascending' : 'descending');
                        const go = () => this._sortBy(c.key);
                        th.addEventListener('click', go);
                        th.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } });
                    }
                    tr.appendChild(th);
                });
                thead.appendChild(tr);
                table.appendChild(thead);
            }
            const rows = this._rows || [];
            if (this._rows && rows.length === 0) {
                this.appendChild(PL.emptyStateNode({ title: this._emptyTitle || 'Nothing here yet', desc: this._emptyDesc }));
                return;
            }
            const tbody = el('tbody');
            rows.forEach((row) => {
                const tr = el('tr');
                if (this.hasAttribute('row-clickable')) {
                    tr.className = 'clickable';
                    tr.tabIndex = 0;
                    const emit = () => this.dispatchEvent(new CustomEvent('pl:row-click', { bubbles: true, detail: { row: row } }));
                    tr.addEventListener('click', emit);
                    tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); emit(); } });
                }
                cols.forEach((c) => {
                    const td = el('td');
                    if (c.mono) td.classList.add('mono');
                    if (c.num) td.classList.add('num');
                    if (c.className) td.classList.add(c.className);
                    const content = c.render ? c.render(row) : row[c.key];
                    if (content instanceof Node) td.appendChild(content);
                    else td.textContent = text(content);
                    tr.appendChild(td);
                });
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);
            this.appendChild(table);
        }
        showError(message) { this._errorMsg = message; this.state = 'error'; }
    }

    /* ============================================================
       pl-modal + PL.alert / PL.confirm — focus-trapped dialogs
       ============================================================ */
    function trapFocus(container, e) {
        const focusable = container.querySelectorAll('pl-button, button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])');
        if (!focusable.length) return;
        const first = focusable[0], last = focusable[focusable.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    }

    function openDialog(opts) {
        return new Promise(function (resolve) {
            const prevFocus = document.activeElement;
            const modal = document.createElement('pl-modal');
            modal.setAttribute('open', '');
            modal.setAttribute('role', 'dialog');
            modal.setAttribute('aria-modal', 'true');

            const titleId = 'pl-dlg-title';
            const card = el('div', { class: 'pl-modal-card' });
            if (opts.title) {
                const t = el('div', { class: 'pl-modal-title', id: titleId }, opts.title);
                card.appendChild(t);
                modal.setAttribute('aria-labelledby', titleId);
            }
            card.appendChild(el('div', { class: 'pl-modal-body' }, opts.body));
            const actions = el('div', { class: 'pl-modal-actions' });

            function close(result) {
                document.removeEventListener('keydown', onKey, true);
                modal.remove();
                if (prevFocus && prevFocus.focus) prevFocus.focus();
                resolve(result);
            }
            function onKey(e) {
                if (e.key === 'Escape') { e.preventDefault(); close(opts.confirm ? false : undefined); }
                else if (e.key === 'Tab') trapFocus(card, e);
            }

            if (opts.confirm) {
                const cancel = el('pl-button', { variant: 'outline' }, opts.cancelLabel || 'Cancel');
                cancel.addEventListener('click', () => close(false));
                actions.appendChild(cancel);
            }
            const ok = el('pl-button', { variant: opts.danger ? 'danger' : 'primary' }, opts.okLabel || (opts.confirm ? 'Confirm' : 'OK'));
            ok.addEventListener('click', () => close(opts.confirm ? true : undefined));
            actions.appendChild(ok);

            card.appendChild(actions);
            const scrim = el('div', { class: 'pl-modal-scrim' });
            if (opts.confirm) scrim.addEventListener('click', () => close(false));
            else scrim.addEventListener('click', () => close(undefined));
            modal.appendChild(scrim);
            modal.appendChild(card);
            document.body.appendChild(modal);
            document.addEventListener('keydown', onKey, true);
            requestAnimationFrame(() => ok.focus());
        });
    }

    /* ============================================================
       pl-toast-host + PL.toast
       ============================================================ */
    function toastHost() {
        let host = document.querySelector('pl-toast-host');
        if (!host) {
            host = document.createElement('pl-toast-host');
            host.setAttribute('aria-live', 'polite');
            host.setAttribute('aria-atomic', 'false');
            document.body.appendChild(host);
        }
        return host;
    }
    const TOAST_ICON = { success: '✓', error: '✕', warning: '!', info: 'i' };
    function showToast(message, tone, duration) {
        tone = tone || 'info';
        const host = toastHost();
        const node = el('div', { class: 'pl-toast', role: tone === 'error' ? 'alert' : 'status', dataset: { tone: tone } });
        node.appendChild(el('span', { class: 'pl-toast-icon', 'aria-hidden': 'true' }, TOAST_ICON[tone] || 'i'));
        node.appendChild(el('span', { class: 'pl-toast-msg' }, message));
        const close = el('button', { class: 'pl-toast-close', type: 'button', 'aria-label': 'Dismiss' }, '×');
        node.appendChild(close);
        host.appendChild(node);
        let timer;
        function dismiss() {
            if (node._gone) return; node._gone = true;
            clearTimeout(timer);
            node.setAttribute('data-leaving', '');
            node.addEventListener('animationend', () => node.remove(), { once: true });
            setTimeout(() => node.remove(), 400);
        }
        close.addEventListener('click', dismiss);
        node.addEventListener('mouseenter', () => clearTimeout(timer));
        node.addEventListener('mouseleave', () => { timer = setTimeout(dismiss, 2500); });
        const ms = duration == null ? (tone === 'error' ? 7000 : 4000) : duration;
        if (ms > 0) timer = setTimeout(dismiss, ms);
        return dismiss;
    }

    /* ============================================================
       PL runtime + state helpers
       ============================================================ */
    const PL = {
        text: text,
        el: el,
        escape: text, // back-compat name for safe text

        toast(message, tone, duration) { return showToast(text(message), tone, duration); },
        alert(arg) {
            const o = typeof arg === 'string' ? { body: arg } : (arg || {});
            return openDialog({ title: o.title, body: o.body, okLabel: o.okLabel });
        },
        confirm(arg) {
            const o = typeof arg === 'string' ? { body: arg } : (arg || {});
            return openDialog({ confirm: true, title: o.title || 'Please confirm', body: o.body, okLabel: o.okLabel, cancelLabel: o.cancelLabel, danger: o.danger });
        },

        /* Skeleton placeholder rows inside any container */
        skeleton(container, opts) {
            opts = opts || {};
            if (!container) return;
            container.textContent = '';
            container.setAttribute('aria-busy', 'true');
            const n = opts.rows || 4;
            for (let i = 0; i < n; i++) container.appendChild(el('div', { class: 'skeleton skeleton-row', 'aria-hidden': 'true' }));
        },
        skeletonTable(colCount, rowCount, cols) {
            const table = el('table');
            const tbody = el('tbody');
            for (let r = 0; r < rowCount; r++) {
                const tr = el('tr');
                for (let c = 0; c < colCount; c++) {
                    const td = el('td');
                    td.appendChild(el('div', { class: 'skeleton skeleton-text', 'aria-hidden': 'true' }));
                    tr.appendChild(td);
                }
                tbody.appendChild(tr);
            }
            table.appendChild(tbody);
            table.setAttribute('aria-busy', 'true');
            return table;
        },
        emptyStateNode(opts) {
            opts = opts || {};
            const node = el('div', { class: 'empty-state', role: 'status' });
            node.appendChild(el('div', { class: 'pl-state-icon', 'aria-hidden': 'true' }, opts.icon || '∅'));
            node.appendChild(el('div', { class: 'pl-state-title' }, opts.title || 'Nothing here yet'));
            if (opts.desc) node.appendChild(el('div', { class: 'pl-state-desc' }, opts.desc));
            if (opts.action) node.appendChild(opts.action);
            return node;
        },
        empty(container, opts) {
            if (!container) return;
            container.removeAttribute('aria-busy');
            container.textContent = '';
            container.appendChild(PL.emptyStateNode(opts));
        },
        errorStateNode(opts) {
            opts = opts || {};
            const node = el('div', { class: 'error-state', role: 'alert' });
            node.appendChild(el('div', { class: 'pl-state-icon', 'aria-hidden': 'true' }, '!'));
            node.appendChild(el('div', { class: 'pl-state-title' }, opts.title || 'Something went wrong'));
            node.appendChild(el('div', { class: 'pl-state-desc' }, opts.message || 'Could not load data.'));
            if (opts.onRetry) {
                const btn = el('pl-button', { variant: 'outline', size: 'sm' }, 'Retry');
                btn.addEventListener('click', opts.onRetry);
                node.appendChild(btn);
            }
            return node;
        },
        error(container, opts) {
            if (!container) return;
            container.removeAttribute('aria-busy');
            container.textContent = '';
            container.appendChild(PL.errorStateNode(opts));
        },
        /* badge node helper for table cells */
        badge(label, tone) {
            const map = { success: 'badge-success', danger: 'badge-danger', warning: 'badge-warning', info: 'badge-info', neutral: 'badge-neutral' };
            return el('span', { class: 'badge ' + (map[tone] || 'badge-neutral') }, label);
        }
    };

    window.PL = PL;

    /* ---------- action delegation: [data-action] replaces inline onclick ---------- */
    document.addEventListener('click', function (e) {
        const target = e.target.closest && e.target.closest('[data-action]');
        if (!target) return;
        if (target.hasAttribute('disabled') || target.getAttribute('aria-disabled') === 'true') return;
        const action = target.dataset.action;
        const fn = window[action];
        if (typeof fn !== 'function') { console.warn('pl: unknown action', action); return; }
        let args = [];
        if (target.dataset.args) {
            try { args = JSON.parse(target.dataset.args); } catch (_) { args = [target.dataset.args]; }
            if (!Array.isArray(args)) args = [args];
        }
        e.preventDefault();
        Promise.resolve().then(async function () {
            if (target.dataset.confirm) {
                const ok = await PL.confirm({ body: target.dataset.confirm, danger: target.dataset.confirmDanger === 'true', okLabel: target.dataset.confirmLabel });
                if (!ok) return;
            }
            return fn.apply(target, args);
        }).catch(function (err) {
            console.error(err);
            PL.toast(err && err.message ? err.message : String(err), 'error');
        });
    });

    /* ---------- form submit delegation: <form data-action="fn"> (Enter-to-search) ---------- */
    document.addEventListener('submit', function (e) {
        const form = e.target;
        if (!form || !form.dataset || !form.dataset.action) return;
        e.preventDefault();
        const fn = window[form.dataset.action];
        if (typeof fn !== 'function') return;
        Promise.resolve(fn.call(form)).catch(function (err) {
            console.error(err);
            PL.toast(err && err.message ? err.message : String(err), 'error');
        });
    });

    /* ============================================================
       SVG helper (el() only creates HTML elements)
       ============================================================ */
    const SVG_NS = 'http://www.w3.org/2000/svg';
    function svgEl(tag, attrs, children) {
        const node = document.createElementNS(SVG_NS, tag);
        if (attrs) for (const k in attrs) node.setAttribute(k, attrs[k]);
        if (children != null) {
            (Array.isArray(children) ? children : [children]).forEach(function (c) {
                if (c != null) node.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
            });
        }
        return node;
    }
    function clamp01(n) { n = Number(n); return n < 0 ? 0 : (n > 1 ? 1 : (isNaN(n) ? 0 : n)); }

    /* ============================================================
       pl-slider — accessible range with live readout
       attrs: min, max, step, value, label, format (ratio|int), disabled
       ============================================================ */
    class PlSlider extends HTMLElement {
        connectedCallback() {
            if (this._built) return;
            this._built = true;
            this.classList.add('pl-slider');
            const id = this.getAttribute('id');
            if (id) this.removeAttribute('id');
            const label = this.getAttribute('label');
            if (label) this.appendChild(el('span', { class: 'pl-slider-label' }, label));
            this._input = el('input', {
                type: 'range',
                min: this.getAttribute('min') || '0',
                max: this.getAttribute('max') || '100',
                step: this.getAttribute('step') || '1',
                value: this.getAttribute('value') || this.getAttribute('min') || '0'
            });
            if (id) this._input.id = id;
            if (this.hasAttribute('disabled')) this._input.disabled = true;
            this._out = el('span', { class: 'pl-slider-out' }, this._fmt(this._input.value));
            this.appendChild(this._input);
            this.appendChild(this._out);
            this._input.addEventListener('input', () => {
                this._out.textContent = this._fmt(this._input.value);
                this.dispatchEvent(new CustomEvent('pl:input', { bubbles: true, detail: { value: this.value } }));
            });
            this._input.addEventListener('change', () => {
                this.dispatchEvent(new CustomEvent('pl:change', { bubbles: true, detail: { value: this.value } }));
            });
        }
        _fmt(v) {
            return this.getAttribute('format') === 'ratio' ? (Number(v) / 100).toFixed(2) : String(Math.round(Number(v)));
        }
        get value() { return this._input ? Number(this._input.value) : Number(this.getAttribute('value') || 0); }
        set value(v) {
            if (this._input) { this._input.value = v; this._out.textContent = this._fmt(v); }
            else this.setAttribute('value', v);
        }
    }

    /* ============================================================
       pl-drawer — normal-flow side panel (no position:fixed)
       ============================================================ */
    class PlDrawer extends HTMLElement {
        connectedCallback() {
            if (this._built) return;
            this._built = true;
            this.classList.add('pl-drawer');
            if (!this.hasAttribute('open')) this.hidden = true;
            this.addEventListener('keydown', (e) => {
                if (e.key === 'Escape' && this.open) { this.open = false; this.dispatchEvent(new CustomEvent('pl:close', { bubbles: true })); }
            });
        }
        get open() { return !this.hidden; }
        set open(v) {
            this.hidden = !v;
            if (v) this.setAttribute('open', ''); else this.removeAttribute('open');
        }
    }

    /* ============================================================
       pl-graph — SVG one-hop neighbourhood (data prop: {nodes, edges, center})
       node: {id, label, card_type, salience, competing}
       edge: {from, to, relation, confidence}
       emits pl:node-click {id}
       ============================================================ */
    class PlGraph extends HTMLElement {
        connectedCallback() { this.classList.add('pl-graph'); if (this._data) this.render(); }
        set data(d) { this._data = d; if (this.isConnected) this.render(); }
        get data() { return this._data; }
        render() {
            this.textContent = '';
            const d = this._data || {};
            const nodes = d.nodes || [];
            if (!nodes.length) return;
            const W = 680, H = 360, cx = W / 2, cy = H / 2, R = 128;
            const svg = svgEl('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'aria-label': 'Memory graph neighbourhood' });
            const centerId = d.center != null ? d.center : nodes[0].id;
            const peers = nodes.filter((n) => n.id !== centerId);
            const pos = {};
            pos[centerId] = { x: cx, y: cy };
            peers.forEach((n, i) => {
                const a = (i / Math.max(1, peers.length)) * Math.PI * 2 - Math.PI / 2;
                pos[n.id] = { x: cx + Math.cos(a) * R, y: cy + Math.sin(a) * R };
            });
            (d.edges || []).forEach((e) => {
                const a = pos[e.from], b = pos[e.to];
                if (!a || !b) return;
                const w = (1.5 + clamp01(e.confidence) * 3).toFixed(1);
                svg.appendChild(svgEl('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, style: 'stroke: var(--c-relation-' + (e.relation || 'same_topic') + '); stroke-width: ' + w }));
                svg.appendChild(svgEl('text', { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 - 3, 'text-anchor': 'middle', class: 'pl-graph-edge-label' }, clamp01(e.confidence).toFixed(2)));
            });
            nodes.forEach((n) => {
                const p = pos[n.id];
                if (!p) return;
                const r = 16 + clamp01(n.salience == null ? 0.5 : n.salience) * 14;
                const g = svgEl('g', { class: 'pl-graph-node' });
                let style = 'fill: var(--c-cardtype-' + (n.card_type || 'preference') + ')';
                if (n.competing) style += '; stroke: var(--c-status-competing); stroke-width: 3';
                g.appendChild(svgEl('circle', { cx: p.x, cy: p.y, r: r.toFixed(1), style: style }));
                const lbl = String(n.label || '');
                g.appendChild(svgEl('text', { x: p.x, y: p.y + r + 13, 'text-anchor': 'middle', class: 'pl-graph-node-label' }, lbl.length > 18 ? lbl.slice(0, 17) + '…' : lbl));
                g.addEventListener('click', () => this.dispatchEvent(new CustomEvent('pl:node-click', { bubbles: true, detail: { id: n.id } })));
                svg.appendChild(g);
            });
            this.appendChild(svg);
        }
    }

    /* ============================================================
       pl-timeline — SVG bitemporal swimlanes (data prop)
       data: {lanes:[{key,label}], items:[{id,lane,label,card_type,validFrom,validUntil,recordedAt,status,competing}], now, asOf}
       emits pl:item-click {id}, pl:asof-change {date}
       ============================================================ */
    class PlTimeline extends HTMLElement {
        connectedCallback() { this.classList.add('pl-timeline'); if (this._data) this.render(); }
        set data(d) { this._data = d; if (this.isConnected) this.render(); }
        get data() { return this._data; }
        render() {
            this.textContent = '';
            const d = this._data || {};
            const lanes = d.lanes || [];
            const items = d.items || [];
            if (!lanes.length) return;
            const W = 680, gutter = 100, top = 26, laneH = 34;
            const H = top + lanes.length * laneH + 22;
            const nowMs = d.now ? Date.parse(d.now) : Date.now();
            const times = [nowMs];
            items.forEach((it) => {
                if (it.validFrom) times.push(Date.parse(it.validFrom));
                if (it.validUntil) times.push(Date.parse(it.validUntil));
                if (it.recordedAt) times.push(Date.parse(it.recordedAt));
            });
            let tmin = Math.min.apply(null, times), tmax = Math.max.apply(null, times);
            if (!(tmax > tmin)) tmax = tmin + 86400000;
            const pad = (tmax - tmin) * 0.05;
            tmin -= pad; tmax += pad;
            const span = tmax - tmin, plot = W - gutter - 12;
            const x = (t) => gutter + ((t - tmin) / span) * plot;
            const laneIndex = {};
            lanes.forEach((l, i) => { laneIndex[l.key] = i; });
            const svg = svgEl('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'aria-label': 'Memory bitemporal timeline' });
            lanes.forEach((l, i) => {
                svg.appendChild(svgEl('text', { x: 8, y: top + i * laneH + laneH / 2 + 3, class: 'pl-timeline-label' }, l.label));
            });
            items.forEach((it) => {
                const li = laneIndex[it.lane];
                if (li == null) return;
                const y = top + li * laneH + 8;
                const x1 = x(Date.parse(it.validFrom || d.now) || nowMs);
                const x2 = x(it.validUntil ? Date.parse(it.validUntil) : nowMs);
                const muted = it.status === 'superseded' || it.status === 'deleted';
                let style = muted ? 'fill: var(--c-text-muted)' : 'fill: var(--c-cardtype-' + (it.card_type || 'preference') + ')';
                if (it.competing) style += '; stroke: var(--c-status-competing); stroke-width: 2';
                const rect = svgEl('rect', { x: x1, y: y, width: Math.max(3, x2 - x1).toFixed(1), height: 18, rx: 4, style: style });
                rect.style.cursor = 'pointer';
                rect.addEventListener('click', (e) => { e.stopPropagation(); this.dispatchEvent(new CustomEvent('pl:item-click', { bubbles: true, detail: { id: it.id } })); });
                svg.appendChild(rect);
                if (it.recordedAt) {
                    const rx = x(Date.parse(it.recordedAt));
                    svg.appendChild(svgEl('polygon', { points: (rx - 4) + ',' + (y - 6) + ' ' + (rx + 4) + ',' + (y - 6) + ' ' + rx + ',' + y, style: 'fill: var(--c-text-sec)' }));
                }
            });
            const nx = x(nowMs);
            svg.appendChild(svgEl('line', { x1: nx, y1: top - 8, x2: nx, y2: H - 14, class: 'pl-timeline-now' }));
            if (d.asOf) {
                const ax = x(Date.parse(d.asOf));
                svg.appendChild(svgEl('line', { x1: ax, y1: top - 8, x2: ax, y2: H - 14, class: 'pl-timeline-asof' }));
            }
            svg.addEventListener('click', (e) => {
                const box = svg.getBoundingClientRect();
                const px = (e.clientX - box.left) / box.width * W;
                if (px < gutter) return;
                const t = tmin + ((px - gutter) / plot) * span;
                this.dispatchEvent(new CustomEvent('pl:asof-change', { bubbles: true, detail: { date: new Date(t).toISOString() } }));
            });
            this.appendChild(svg);
        }
    }

    /* ---------- register ---------- */
    customElements.define('pl-button', PlButton);
    customElements.define('pl-button-group', PlButtonGroup);
    customElements.define('pl-input', PlInput);
    customElements.define('pl-textarea', PlTextarea);
    customElements.define('pl-select', PlSelect);
    customElements.define('pl-field-group', PlFieldGroup);
    customElements.define('pl-toggle', PlToggle);
    customElements.define('pl-table', PlTable);
    customElements.define('pl-modal', class extends HTMLElement { });
    customElements.define('pl-toast-host', class extends HTMLElement { });
    customElements.define('pl-slider', PlSlider);
    customElements.define('pl-drawer', PlDrawer);
    customElements.define('pl-graph', PlGraph);
    customElements.define('pl-timeline', PlTimeline);
})();
