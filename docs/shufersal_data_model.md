# מודל הנתונים של שופרסל — Shufersal Data Model

## רקע: חוק שקיפות המחירים (2015)

חוק המזון (תיקון מס' 2, 2014) מחייב רשתות מזון ישראליות לפרסם מחירים בפומבי.
שופרסל מפרסמת קבצי XML דחוסים (GZ) ב-`prices.shufersal.co.il` על בסיס יומי.

---

## סוגי קבצים

### PriceFull — קובץ מחירים מלא

- **מכיל:** כל המוצרים הנמכרים בסניף — עם מחיר המדף הקטלוגי
- **תדירות:** פעם ביום (בדרך כלל בשעות הבוקר)
- **XML:** עצי `<Item>` עם `<ItemCode>`, `<ItemName>`, `<ItemPrice>`, `<ManufacturerName>`
- **שימוש במערכת:** בסיס לטבלת `products` — `price` = מחיר קטלוגי, `promo_price = None`

```xml
<Item>
  <ItemCode>7290000000001</ItemCode>
  <ItemName>קוקה קולה 1.5 ל</ItemName>
  <ItemPrice>8.90</ItemPrice>
  <ManufacturerName>קוקה קולה ישראל</ManufacturerName>
</Item>
```

### PromoFull — קובץ מבצעים מלא

- **מכיל:** **רק** פריטים הנמצאים כרגע במבצע פעיל
- **תדירות:** פעם ביום
- **XML:** עצי `<Promotion>` עם `<DiscountedPrice>`/`<PromotionPrice>`, ובתוכם `<Item>` אחד או יותר
- **שימוש במערכת:** overlay — מוסיף `promo_price` לרשומות קיימות לפי (barcode, format_name)

```xml
<Promotion>
  <PromotionPrice>6.90</PromotionPrice>
  <MinQty>2</MinQty>
  <Item>
    <ItemCode>7290000000001</ItemCode>
    <ItemPrice>8.90</ItemPrice>  <!-- מחיר רגיל, לעיתים מופיע גם בפרומו -->
  </Item>
</Promotion>
```

**הערה:** `MinQty > 1` מייצג מבצע חבילה — המחיר מחולק ל-`MinQty` לקבלת מחיר ליחידה.

### Price / Promo (delta)

- גרסאות "דלתא" — עדכוני מחיר ומבצע תוך-יומיים
- מבנה XML זהה ל-PriceFull / PromoFull
- פחות מקיפים — משמשים לגילוי "נתונים חדשים" (freshness check) ולbackfill כאשר Full חסר

### Stores

- כל הסניפים עם `SubChainName` (= פורמט הצרכני, כגון "שופרסל אקספרס")
- משמש למיפוי `store_type → format_name` לפי `constants.FORMAT_KEYWORDS`

---

## אסטרטגיית ה-Pipeline

### בעיה שהיתה (לפני התיקון)

הסקרייפר בחר **רק** PromoFull (עצר ב-`break` לאחר הסוג הראשון שנמצא).
כתוצאה — כל הרשומות ב-DB הגיעו מ-PromoFull → לכל מוצר היה `promo_price` מוגדר
→ הממשק הציג "כל המוצרים במבצע".

### הפתרון הנוכחי

```
scraper_agent.run()
  ├── promo_files = PromoFull files (up to 3 per consumer format)
  └── price_files = PriceFull files (up to 3 per consumer format)

parser_agent.run(promo_files, price_files)
  ├── parse PriceFull  → price_records  [{price=X, promo_price=None}]
  ├── parse PromoFull  → promo_records  [{price=X, promo_price=Y}]
  └── _merge_price_promo(price_records, promo_records)
        ├── PriceFull is the base (all products)
        ├── PromoFull overlays promo_price where barcode+format match
        └── Items only in PromoFull are included as-is (edge case)

db.replace_products(merged_records)
  → products with promo_price=None  → מחיר קטלוגי בלבד (כחול ב-UI)
  → products with promo_price set   → נמצא במבצע (ירוק ב-UI)
```

---

## מיפוי פורמטים צרכניים

`constants.FORMAT_KEYWORDS` מגדיר 8 פורמטים צרכניים:
- שופרסל אקספרס, שופרסל שלי, שופרסל BE, שופרסל אונליין, יש בשכונה, יש חסד, שופרסל דיל, שופרסל

`map_consumer_format(store_type)` ממפה את `SubChainName` חופשי → שם פורמט קנוני.

---

## נקודות לתשומת לב

1. **3 קבצים לפורמט בכל ריצה** — הבחירה מוגבלת ל-3 כדי לא להוריד עשרות קבצים זהים.
2. **Freshness check** מבוסס על טיימסטמפ של קבצי Delta (עמוד 1), לא Full — כי Full נמצא בעמודים האחרונים.
3. **Fallback**: אם PriceFull לא נמצא ב-60 העמודים האחרונים, חוזר לעמוד 1 לחיפוש Price delta.
4. **Bundle promotions**: `MinQty > 1` — מחיר מחולק ליחידה. דוגמה: "2 ב-10₪" → `promo_price = 5.0`.
