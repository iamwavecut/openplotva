#!/usr/bin/env python3
"""Draw the synthetic images and clip the vision fixtures point at.

    python3 tools/prompt-eval/make_vision_media.py [--out DIR] [--sizes 512,1024]

Needs Pillow; scikit-image adds its public-domain sample photos, and ffmpeg
the video. Writes the originals to DIR (default `local/vision/`) and, for every
size, `DIR-<size>/` with each still scaled the way the bot sends it: longest
side at most <size>, stickers at most 512, JPEG sources re-encoded at quality
90 and the rest as PNG."""
from __future__ import annotations

import argparse
import math
import random
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "/Library/Fonts/Arial Unicode.ttf",
]
BOLD_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]
STICKER_SIDE = 512


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    for path in (BOLD_CANDIDATES if bold else []) + FONT_CANDIDATES:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    raise SystemExit("no Cyrillic TrueType font found; install fonts-dejavu-core")


def centered(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], text: str, face, fill) -> None:
    left, top, right, bottom = draw.textbbox((0, 0), text, font=face)
    x = box[0] + (box[2] - box[0] - (right - left)) // 2
    y = box[1] + (box[3] - box[1] - (bottom - top)) // 2
    draw.text((x, y), text, font=face, fill=fill)


def sign(out: Path) -> None:
    img = Image.new("RGB", (1600, 1200), (118, 110, 98))
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 0, 1600, 1200), fill=(92, 84, 76))
    draw.rectangle((380, 180, 1220, 1020), fill=(160, 150, 138))
    draw.rectangle((470, 330, 1130, 800), fill=(250, 250, 245), outline=(200, 30, 30), width=12)
    centered(draw, (470, 360, 1130, 470), "ЗАКРЫТО НА РЕМОНТ", font(52, True), (200, 30, 30))
    centered(draw, (470, 480, 1130, 570), "до 15 октября", font(44), (30, 30, 30))
    centered(draw, (470, 620, 1130, 690), "Приносим извинения", font(30), (60, 60, 60))
    centered(draw, (470, 690, 1130, 760), "за неудобства", font(30), (60, 60, 60))
    draw.ellipse((1150, 560, 1190, 600), fill=(210, 180, 60))
    img.save(out / "sign-ru.jpg", quality=92)


def chat_screenshot(out: Path) -> None:
    img = Image.new("RGB", (1080, 1920), (231, 235, 240))
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 0, 1080, 150), fill=(82, 136, 193))
    draw.text((40, 50), "Дача 2026", font=font(44, True), fill=(255, 255, 255))
    draw.text((40, 105), "5 участников", font=font(26), fill=(220, 230, 245))
    messages = [
        ("Марина", "18:02", "Кто завтра едет на дачу?", False),
        ("Олег", "18:05", "Я, но только после обеда", False),
        ("Марина", "18:06", "Возьми мангал из гаража", False),
        ("Олег", "18:07", "Ок, и угли куплю по дороге", False),
        ("Вы", "18:10", "Я привезу салат и лимонад", True),
        ("Света", "18:12", "Мы с детьми к 15:00", False),
        ("Марина", "18:13", "Отлично, ключи у соседки", False),
        ("Вы", "18:15", "Напомните адрес: СНТ Рассвет, участок 42?", True),
    ]
    y = 190
    body, meta = font(30), font(22)
    for name, clock, text, mine in messages:
        width = min(760, int(draw.textlength(text, font=body)) + 60)
        x = 1080 - width - 30 if mine else 30
        draw.rounded_rectangle((x, y, x + width, y + 150), radius=26, fill=(214, 240, 200) if mine else (255, 255, 255))
        draw.text((x + 26, y + 16), name, font=meta, fill=(82, 136, 193))
        draw.text((x + 26, y + 52), text, font=body, fill=(20, 20, 20))
        draw.text((x + width - 90, y + 108), clock, font=meta, fill=(140, 140, 140))
        y += 180
    draw.rectangle((0, 1800, 1080, 1920), fill=(255, 255, 255))
    draw.text((40, 1840), "Сообщение", font=font(30), fill=(160, 160, 160))
    img.save(out / "chat-screenshot-ru.png")


def document(out: Path) -> None:
    img = Image.new("RGB", (1240, 1754), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    centered(draw, (0, 100, 1240, 170), "ДОГОВОР АРЕНДЫ № 17/2026", font(40, True), (0, 0, 0))
    centered(draw, (0, 180, 1240, 220), "г. Самара, 3 сентября 2026 г.", font(24), (0, 0, 0))
    paragraphs = [
        "1. Арендодатель передаёт, а Арендатор принимает во временное пользование",
        "квартиру по адресу: ул. Примерная, д. 8, кв. 31, общей площадью 42 кв. м.",
        "2. Срок аренды: 11 месяцев с 1 октября 2026 года.",
        "3. Арендная плата составляет 45 000 рублей в месяц и вносится",
        "не позднее 5 числа каждого месяца.",
        "4. Залог в размере 45 000 рублей возвращается по окончании срока.",
        "5. Коммунальные платежи оплачивает Арендатор.",
    ]
    y = 300
    for line in paragraphs:
        draw.text((110, y), line, font=font(24), fill=(0, 0, 0))
        y += 48
    draw.text((110, 1500), "Арендодатель: ____________", font=font(24), fill=(0, 0, 0))
    draw.text((680, 1500), "Арендатор: ____________", font=font(24), fill=(0, 0, 0))
    img.save(out / "document-ru.png")


def meme(out: Path) -> None:
    img = Image.new("RGB", (1000, 1000), (70, 120, 170))
    draw = ImageDraw.Draw(img)
    draw.ellipse((300, 330, 700, 730), fill=(240, 190, 120))
    draw.ellipse((400, 460, 450, 510), fill=(30, 30, 30))
    draw.ellipse((550, 460, 600, 510), fill=(30, 30, 30))
    draw.arc((410, 520, 590, 650), start=20, end=160, fill=(30, 30, 30), width=8)
    for text, top in (("КОГДА НАПИСАЛ КОД", 60), ("И ОН СРАЗУ ЗАРАБОТАЛ", 850)):
        face = font(64, True)
        left, _, right, _ = draw.textbbox((0, 0), text, font=face)
        x = (1000 - (right - left)) // 2
        draw.text((x, top), text, font=face, fill=(255, 255, 255), stroke_width=5, stroke_fill=(0, 0, 0))
    img.save(out / "meme-ru.jpg", quality=92)


def receipt(out: Path) -> None:
    img = Image.new("RGB", (640, 1300), (252, 252, 248))
    draw = ImageDraw.Draw(img)
    centered(draw, (0, 40, 640, 100), "ПРОДУКТЫ 24", font(40, True), (0, 0, 0))
    centered(draw, (0, 100, 640, 140), "Кассовый чек № 0417", font(24), (0, 0, 0))
    rows = [("Молоко 3,2% 1 л", "89,90"), ("Хлеб бородинский", "54,00"), ("Сыр российский 0,35 кг", "245,70"),
            ("Пакет", "9,99")]
    y = 220
    for name, price in rows:
        draw.text((40, y), name, font=font(26), fill=(0, 0, 0))
        draw.text((600 - draw.textlength(price, font=font(26)), y), price, font=font(26), fill=(0, 0, 0))
        y += 60
    draw.line((40, y + 10, 600, y + 10), fill=(0, 0, 0), width=2)
    draw.text((40, y + 40), "ИТОГО", font=font(34, True), fill=(0, 0, 0))
    draw.text((600 - draw.textlength("399,59", font=font(34, True)), y + 40), "399,59", font=font(34, True), fill=(0, 0, 0))
    draw.text((40, y + 120), "Спасибо за покупку!", font=font(24), fill=(0, 0, 0))
    img.save(out / "receipt-ru.png")


def counting(out: Path) -> None:
    rng = random.Random(29)
    img = Image.new("RGB", (1200, 900), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    spots: list[tuple[int, int]] = []
    while len(spots) < 11:
        spot = (rng.randint(80, 1120), rng.randint(80, 820))
        if all(math.dist(spot, other) > 170 for other in spots):
            spots.append(spot)
    for index, (x, y) in enumerate(spots):
        if index < 7:
            draw.ellipse((x - 55, y - 55, x + 55, y + 55), fill=(215, 40, 40))
        else:
            draw.rectangle((x - 50, y - 50, x + 50, y + 50), fill=(40, 80, 215))
    img.save(out / "counting.png")


def chart(out: Path) -> None:
    img = Image.new("RGB", (1400, 1000), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    centered(draw, (0, 30, 1400, 100), "Продажи по кварталам, 2026 (тыс. шт.)", font(40, True), (20, 20, 20))
    base, left = 880, 180
    draw.line((left, 150, left, base), fill=(0, 0, 0), width=3)
    draw.line((left, base, 1300, base), fill=(0, 0, 0), width=3)
    for index, (label, value) in enumerate((("I кв.", 120), ("II кв.", 95), ("III кв.", 140), ("IV кв.", 160))):
        x = left + 80 + index * 260
        top = base - value * 4
        draw.rectangle((x, top, x + 150, base), fill=(60, 140, 90))
        centered(draw, (x, top - 50, x + 150, top - 5), str(value), font(32, True), (20, 20, 20))
        centered(draw, (x, base + 10, x + 150, base + 60), label, font(30), (20, 20, 20))
    img.save(out / "chart-ru.png")


def drawing(out: Path) -> None:
    img = Image.new("RGB", (1200, 900), (170, 215, 250))
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 620, 1200, 900), fill=(110, 180, 90))
    draw.ellipse((980, 70, 1120, 210), fill=(255, 215, 60))
    draw.rectangle((420, 380, 760, 640), fill=(200, 120, 80))
    draw.polygon(((390, 390), (590, 230), (790, 390)), fill=(150, 50, 40))
    draw.rectangle((550, 500, 630, 640), fill=(100, 60, 30))
    draw.rectangle((460, 430, 530, 490), fill=(200, 230, 250))
    for x in (130, 260, 950):
        draw.rectangle((x - 15, 520, x + 15, 650), fill=(110, 70, 40))
        draw.ellipse((x - 80, 380, x + 80, 560), fill=(40, 130, 60))
    img.save(out / "drawing-house.png")


def ui_english(out: Path) -> None:
    img = Image.new("RGB", (1080, 1400), (242, 242, 247))
    draw = ImageDraw.Draw(img)
    draw.text((50, 60), "Settings", font=font(56, True), fill=(0, 0, 0))
    rows = [("Wi-Fi", "HomeNet-5G"), ("Bluetooth", "Off"), ("Battery", "42%"), ("Do Not Disturb", "22:00 – 07:00"),
            ("Software Update", "Version 18.2 available")]
    y = 200
    for name, value in rows:
        draw.rounded_rectangle((40, y, 1040, y + 110), radius=20, fill=(255, 255, 255))
        draw.text((80, y + 35), name, font=font(34), fill=(0, 0, 0))
        draw.text((1000 - draw.textlength(value, font=font(30)), y + 38), value, font=font(30), fill=(120, 120, 120))
        y += 140
    img.save(out / "ui-en.png")


def small_text(out: Path) -> None:
    img = Image.new("RGB", (2400, 1600), (245, 240, 225))
    draw = ImageDraw.Draw(img)
    draw.text((120, 100), "ОБЪЯВЛЕНИЕ ДЛЯ ЖИЛЬЦОВ", font=font(64, True), fill=(20, 20, 20))
    lines = [
        "22 сентября с 10:00 до 16:00 будет отключена горячая вода.",
        "Лифт во втором подъезде работает только до 9 этажа.",
        "Собрание собственников — 30 сентября в 19:00, холл первого этажа.",
        "Вопросы: управляющая компания, тел. 123-45-67.",
    ]
    y = 300
    for line in lines:
        draw.text((120, y), line, font=font(40), fill=(30, 30, 30))
        y += 90
    img.save(out / "notice-small-text-ru.jpg", quality=92)


def dense_invoice(out: Path) -> None:
    img = Image.new("RGB", (1240, 1754), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    face = font(20)
    lines = [
        "ООО «Северный ветер», ИНН 1234567890, КПП 123401001",
        "Счёт на оплату № 4471-Б от 19 сентября 2026 г.",
        "Получатель: ООО «Северный ветер», р/с 40702810000000000001",
        "Плательщик: ИП Иванова А. А., ул. Примерная, д. 1",
    ]
    lines += [f"{n}. Позиция каталога {1000 + n * 17}, кол-во {n % 4 + 1} шт., цена {n * 350},00 руб." for n in range(1, 25)]
    lines += ["Итого к оплате: 128 450,00 руб. (сто двадцать восемь тысяч четыреста пятьдесят рублей)",
              "Срок оплаты: до 24.10.2026. Назначение платежа: оплата по счёту № 4471-Б."]
    y = 80
    for line in lines:
        draw.text((80, y), line, font=face, fill=(0, 0, 0))
        y += 34
    img.save(out / "dense-invoice-ru.png")


def dense_settings(out: Path) -> None:
    img = Image.new("RGB", (1080, 2400), (248, 248, 250))
    draw = ImageDraw.Draw(img)
    draw.text((50, 60), "Настройки экрана", font=font(40, True), fill=(0, 0, 0))
    rows = [("Яркость", "64%"), ("Автоблокировка", "30 сек"), ("Тёмная тема", "с 21:30 до 07:00"),
            ("Размер шрифта", "Средний"), ("Частота обновления", "120 Гц"), ("Ночной режим", "выключен"),
            ("Заставка", "Часы"), ("Масштаб интерфейса", "110%"), ("Поворот экрана", "Авто"),
            ("Всегда на экране", "только уведомления"), ("Разрешение", "2400 × 1080"), ("Цветовой профиль", "Яркий")]
    y = 160
    for name, value in rows:
        draw.rounded_rectangle((30, y, 1050, y + 150), radius=18, fill=(255, 255, 255))
        draw.text((60, y + 30), name, font=font(26), fill=(0, 0, 0))
        draw.text((60, y + 80), value, font=font(22), fill=(110, 110, 110))
        y += 180
    img.save(out / "dense-settings-ru.png")


def sticker(out: Path) -> None:
    img = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse((56, 56, 456, 456), fill=(255, 205, 60, 255), outline=(40, 40, 40, 255), width=10)
    draw.ellipse((170, 180, 220, 240), fill=(40, 40, 40, 255))
    draw.ellipse((292, 180, 342, 240), fill=(40, 40, 40, 255))
    draw.arc((160, 250, 352, 380), start=15, end=165, fill=(40, 40, 40, 255), width=12)
    draw.text((150, 400), "ПРИВЕТ!", font=font(56, True), fill=(220, 40, 40, 255), stroke_width=4,
              stroke_fill=(255, 255, 255, 255))
    img.save(out / "sticker-hello.webp")


def sample_photos(out: Path) -> None:
    try:
        from skimage import data
    except ImportError:
        print("scikit-image missing: sample photos skipped")
        return
    for name in ("astronaut", "coffee", "chelsea", "rocket"):
        Image.fromarray(getattr(data, name)()).convert("RGB").save(out / f"photo-{name}.jpg", quality=92)


def video(out: Path) -> None:
    if not shutil.which("ffmpeg"):
        print("ffmpeg missing: video skipped")
        return
    steps = ["ШАГ 1: ОТКРОЙТЕ КРЫШКУ", "ШАГ 2: НАЛЕЙТЕ ВОДУ", "ШАГ 3: НАЖМИТЕ КНОПКУ"]
    colors = [(200, 60, 60), (60, 90, 200), (60, 160, 80)]
    with tempfile.TemporaryDirectory() as tmp:
        frames = Path(tmp)
        for index in range(24 * 4):
            second = index / 4
            step = min(int(second // 8), 2)
            img = Image.new("RGB", (960, 540), (235, 235, 235))
            draw = ImageDraw.Draw(img)
            x = 120 + int((second % 8) * 80)
            if step == 0:
                draw.rectangle((x, 220, x + 140, 360), fill=colors[step])
            elif step == 1:
                draw.ellipse((x, 220, x + 140, 360), fill=colors[step])
            else:
                draw.polygon(((x, 360), (x + 70, 220), (x + 140, 360)), fill=colors[step])
            centered(draw, (0, 40, 960, 120), steps[step], font(44, True), (20, 20, 20))
            img.save(frames / f"f{index:04d}.png")
        subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-y", "-framerate", "4", "-i", str(frames / "f%04d.png"),
             "-c:v", "libx264", "-pix_fmt", "yuv420p", "-r", "24", "-movflags", "+faststart", str(out / "steps-ru.mp4")],
            check=True,
        )


def scaled(src: Path, dest_dir: Path, max_side: int) -> None:
    img = Image.open(src)
    side = STICKER_SIDE if src.suffix == ".webp" else max_side
    width, height = img.size
    if width > side or height > side:
        scale = min(side / width, side / height)
        img = img.resize((max(1, round(width * scale)), max(1, round(height * scale))), Image.Resampling.BICUBIC)
    if src.suffix == ".jpg":
        img.convert("RGB").save(dest_dir / src.name, quality=90)
    elif src.suffix == ".webp":
        img.save(dest_dir / src.name, lossless=True)
    else:
        img.save(dest_dir / src.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=HERE / "local" / "vision")
    parser.add_argument("--sizes", default="512,1024")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for draw_one in (sign, chat_screenshot, document, meme, receipt, counting, chart, drawing, ui_english, small_text,
                     dense_invoice, dense_settings, sticker, sample_photos, video):
        draw_one(args.out)
    for size in (int(part) for part in args.sizes.split(",") if part):
        dest = args.out.parent / f"{args.out.name}-{size}"
        dest.mkdir(parents=True, exist_ok=True)
        for src in sorted(args.out.iterdir()):
            if src.suffix in (".jpg", ".png", ".webp"):
                scaled(src, dest, size)
            elif src.suffix == ".mp4":
                shutil.copy(src, dest / src.name)
    print("media in", args.out)


if __name__ == "__main__":
    main()
