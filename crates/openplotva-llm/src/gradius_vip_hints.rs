//! Cheeky, plain-text VIP hints shown to free users.
//!
//! The app layer HTML-escapes and wraps each hint in `<tg-spoiler>` after the
//! untouched ad body, so every string here must be Telegram-safe: plain text,
//! no `<`, `>`, `&`, and each carries a `VIP` marker. Selected per impression
//! by [`crate::gradius::vip_hint_for_impression`].
//!
//! Tone: plain human sentences, variations of the original line
//! "Кстати, пользователи VIP не видят рекламы". No marketing speak, no
//! typography tricks, no metaphors: the whole message is "buy VIP, no ads".

pub(super) const GRADIUS_VIP_HINTS: [&str; 50] = [
    // — variations of the original "кстати" aside —
    "Кстати, пользователи VIP не видят рекламы",
    "Кстати, в VIP ответы без рекламы",
    "Кстати, у VIP рекламы нет",
    "Кстати говоря, в VIP всё без рекламы",
    "А, кстати: VIP рекламы не видит",
    "Кстати, есть способ не видеть рекламу. VIP",
    "Между прочим, VIP рекламы не видит",
    "К слову, у VIP чистые ответы, без рекламы",
    "Если что, VIP существует. Без рекламы",
    "Кстати, реклама только у не-VIP",
    // — want no ads? simple nudge —
    "Не хочешь видеть рекламу? Возьми VIP",
    "Надоела реклама? Есть VIP",
    "Хочешь ответы без рекламы? Это VIP",
    "Не нравится реклама? Есть решение: VIP",
    "Устал от рекламы? VIP спасает",
    "Бесит реклама? VIP решает",
    "Реклама мешает? VIP её убирает",
    "Хочешь без рекламы? Ну, есть VIP",
    "Достала реклама? VIP в помощь",
    "Мешает реклама? Это поправимо. VIP",
    // — the plain fact: ads are for non-VIP only —
    "Рекламу видят все, кроме VIP",
    "Не видят рекламы только VIP",
    "Реклама показывается всем, кроме VIP",
    "Реклама тут потому, что ты не VIP",
    "У VIP этой рекламы нет",
    "Вся эта реклама только для не-VIP",
    "Рекламы не будет, если взять VIP",
    "VIP не видит ни этой, ни другой рекламы",
    "Убрать рекламу просто: стать VIP",
    "Такое не показывают VIP",
    // — direct nudge: buy VIP, ads gone —
    "Купи VIP и реклама исчезнет",
    "Оформи VIP и рекламы не будет",
    "Стань VIP и реклама уйдёт",
    "Один VIP и никакой рекламы",
    "VIP убирает рекламу полностью",
    "VIP просто скрывает рекламу",
    "Со статусом VIP рекламы нет",
    "Без рекламы можно. Это называется VIP",
    "Хочешь убрать рекламу? Купи VIP",
    "Проще всего убрать рекламу через VIP",
    // — living like a VIP —
    "VIP читают этот ответ без рекламы",
    "VIP сейчас не видят этой рекламы",
    "У VIP всё то же, только без рекламы",
    "VIP даже не в курсе, что тут реклама",
    "Спонсор этого чата не достаёт VIP пользователей",
    "Завидую VIP: у них рекламы нет",
    "Хорошо живётся VIP, рекламы нет",
    "VIP получают ответы без рекламы",
    "Реклама есть у всех. У VIP нет",
    "У VIP тут пусто. Без рекламы",
];
