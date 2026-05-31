# EDA prompt research — xorq codegen test suite

Six real notebooks doing genuine first-contact EDA on unfamiliar datasets.
Source notebooks were read but not executed. The goal is to back out natural
prompts and use them to test xorq expression codegen via the MCP tools.

---

## Six notebooks

### 1. NYC Airbnb 2019

**Source:** https://github.com/rajviishah/Exploratory-Data-Analysis/blob/master/EDA_NYC_Airbnb.ipynb
**Dataset:** NYC Airbnb Open Data 2019 — 48,895 listings, 16 columns

| # | Operation | Natural prompt |
|---|---|---|
| 1 | `isnull().sum()` — 10,052 nulls in `last_review` and `reviews_per_month` | "how many nulls are in each column?" |
| 2 | Fill nulls with domain defaults (0, "NoName", "NotReviewed") | "fill missing reviews_per_month with 0 and missing names with 'NoName'" |
| 3 | `price.describe()` — mean $152, max $10,000 | "give me summary stats for the price column" |
| 4 | Histogram of price, split at $1000 | "show me the distribution of price, then split it at $1000 to see both ends" |
| 5 | `neighbourhood.value_counts()` — 221 unique; Williamsburg leads | "what are the most common neighbourhoods and how many listings does each have?" |
| 6 | `groupby().filter(lambda x: x.count() > 200)` — 87% in high-density areas | "filter to only neighbourhoods with more than 200 listings and count how many listings remain" |
| 7 | `groupby('neighbourhood_group')['price'].mean()` | "group by neighbourhood group and compute the average price for each" |
| 8 | `room_type.value_counts()` + mean price by room type | "show me the count and average price for each room type" |
| 9 | `price.corr(minimum_nights)` — near-zero | "what's the correlation between price and minimum nights?" |

**Pandas-specific (won't map to ibis):** `groupby().filter(lambda)` on group-size predicate, `.apply(len)` on string column, capping outliers via `.loc[(cond), col] = value`.

---

### 2. Superstore Retail Sales

**Source:** https://github.com/AnalystHub-Hub/Superstore-Sales-Analysis/blob/main/SuperStore_data.ipynb
**Dataset:** Sample Superstore — ~10,296 orders, 21 columns (Order Date, Ship Mode, Segment, Region, Category, Sub-Category, Sales, Profit, Discount, Quantity)

| # | Operation | Natural prompt |
|---|---|---|
| 1 | `isnull().sum()` — 302 nulls, then `dropna()` | "how many nulls are there per column, and drop any rows with missing values" |
| 2 | `duplicated().sum()` — zero | "check for duplicate rows" |
| 3 | `describe()` on Sales and Profit — mean sale $229, profit ranging -$6,599 to $8,399 | "give me summary statistics for sales and profit" |
| 4 | Correlation heatmap — strong positive Sales↔Profit, strong negative Discount↔Profit | "show me a correlation heatmap and tell me which variables are most related to profit" |
| 5 | Outlier detection on Sales using IQR — 1,167 records above $1,068 | "find outliers in the sales column using the IQR method" |
| 6 | `groupby('Region')['Profit'].sum().sort_values()` | "which region generates the most total profit?" |
| 7 | `groupby('Sub-Category')[['Sales','Profit']].sum().sort_values('Sales')` | "group by sub-category and show total sales and profit, sorted by sales" |

**Pandas-specific:** subplot loops for frequency bars, box plots per category.

---

### 3. NYC Taxi Trips (Yellow cab 2017)

**Source:** https://chih-ling-hsu.github.io/2018/05/14/NYC
**Dataset:** NYC Yellow Taxi Jan–Jun 2017 — hundreds of millions of rows (loaded in chunks + SQLite); taxi zone shapefile

| # | Operation | Natural prompt |
|---|---|---|
| 1 | Load sample 10 rows, inspect columns | "load a sample of 10 rows and show me the column names and first records" |
| 2 | `trip_distance.describe()` — mean 2.89, max 151 mi, extremely right-skewed | "describe the trip_distance column — what's the mean, median, and max?" |
| 3 | Extract pickup hour from datetime | "parse the pickup datetime and extract just the hour into a new column" |
| 4 | `groupby('borough').agg({'PUcount':'sum'}).sort_values()` — Manhattan 88M, Staten Island 13K | "group trips by borough and count total pickups per borough, sorted descending" |
| 5 | Pickups and dropoffs by hour — peak 6–7PM, trough 5AM | "show me the number of pickups and dropoffs for each hour of the day" |
| 6 | Split trips by distance (`< 30` vs `>= 30` miles), compare OD pairs | "split trips into short and long by distance and compare the most common origin-destination pairs" |
| 7 | `WHERE fare_type = 'negotiated'` — 40% of long trips | "what share of long-distance trips used negotiated fares?" |

**Pandas-specific:** chunked iteration for massive file, substring datetime parsing, shapefile parsing with pyshp/Shapely.

---

### 4. Capital Bikeshare Washington D.C. (2011–2012)

**Source:** https://github.com/fikrionii/Dicoding-Bike-Sharing/blob/main/notebook-bikeshare-analysis.ipynb
**Dataset:** Capital Bikeshare hourly data — 17,379 rows, 17 columns (normalized temp, humidity, windspeed, season codes, casual/registered user counts)

| # | Operation | Natural prompt |
|---|---|---|
| 1 | `isna().sum()` — zero nulls; `duplicated().sum()` — zero | "check for any missing values or duplicate rows" |
| 2 | `describe()` on numeric columns | "give me summary stats for all numeric columns" |
| 3 | Extract year, month name, weekday name from date | "convert the date column to datetime and extract year, month name, and weekday name as new columns" |
| 4 | Map season codes 1–4 to string names | "the season column uses codes 1-4 — replace them with the actual season names" |
| 5 | Denormalize weather: temp × 41, hum × 100 | "the temperature column is normalized 0–1 — convert it back to Celsius by multiplying by 41" |
| 6 | `groupby('season')['cnt'].mean()` | "group by season and compute the average number of rentals" |
| 7 | `groupby('hr')['cnt'].mean()` — hourly usage curve | "group by hour of day and show average rentals — I want to see the usage curve across the day" |
| 8 | Correlation of temp and hum against cnt | "what's the correlation between temperature and total rentals? what about humidity?" |
| 9 | `groupby('weekday')['cnt'].mean()` | "compare average rentals by day of the week" |

**Pandas-specific:** `.dt` accessor chain (maps fine to ibis `t.ts.hour()` etc.), row-by-row season label mapping via Python list (maps to `case().when()`).

---

### 5. Framingham Heart Study

**Source:** https://github.com/GauravPadawe/Framingham-Heart-Study/blob/master/Framingham%20Heart%20Study.py
**Dataset:** Framingham Heart Study — 4,240 patients, 16 features (demographics, clinical measurements, 10-year CHD outcome)

| # | Operation | Natural prompt |
|---|---|---|
| 1 | `isnull().sum()` — glucose 9.15% null, education 2.47%, cigsPerDay 0.68% | "show me the percentage of missing values for each column" |
| 2 | `describe()` on continuous variables — age 32–70, sysBP 83–295 | "give me descriptive statistics for all numeric columns" |
| 3 | `TenYearCHD.value_counts()` — 85:15 class imbalance | "how many patients have the positive outcome vs negative? what's the ratio?" |
| 4 | `groupby('male')['TenYearCHD'].value_counts()` — males have higher CHD rate | "group by gender and show the CHD diagnosis rate for each group" |
| 5 | `groupby('currentSmoker')['cigsPerDay'].median()` | "what's the median cigarettes per day for smokers vs non-smokers?" |
| 6 | Median imputation per group for cigsPerDay and BMI | "fill missing cigsPerDay using the median for each smoker/non-smoker group" |
| 7 | Create age bands (adult/middle-aged/senior) | "bin the age column into three groups: adult, middle-aged, and senior" |

**Pandas-specific:** `.transform('mean')` for group-based imputation in place — maps to ibis window function `mutate(fill=col.mean().over(ibis.window(group_by=...)))`. SMOTE is ML-only.

---

### 6. Melbourne Housing Market

**Source:** https://github.com/dipalira/Melbourne-Housing-Data-Kaggle/blob/master/housing.ipynb
**Dataset:** Melbourne Housing Full — ~34K listings (post-filter ~27K), 21 columns (Suburb, Type, Price, Rooms, Bathroom, Landsize, BuildingArea, YearBuilt, Regionname, coordinates)

| # | Operation | Natural prompt |
|---|---|---|
| 1 | `isnull().sum()` — Price 7,610 nulls, BuildingArea 21,115, YearBuilt 19,306 | "how many nulls are in each column? which columns have the most missing data?" |
| 2 | Drop rows where Price or Regionname is null | "drop any rows where price is missing, and drop rows with no region" |
| 3 | `groupby('Rooms')['Bathroom'].transform('mean')` — fill Bathroom per room count | "fill missing bathroom counts using the average for properties with the same number of rooms" |
| 4 | `groupby('Type')['Car'].transform('mean')` — fill Car spaces by type | "fill missing car spaces using the average for each property type" |
| 5 | `groupby('Regionname')['Price'].mean().sort_values()` — Southern Metropolitan $1.4M, Western Victoria $433K | "group by region and compute the mean sale price per region, sorted high to low" |

**Pandas-specific:** `.transform()` for group-fill maps to ibis window `mutate`; seaborn factorplot doesn't translate.

---

## Cross-notebook operation taxonomy

### Maps cleanly to SQL/ibis

| pandas pattern | ibis equivalent |
|---|---|
| `groupby(col).agg(mean/sum/count)` | `group_by().aggregate()` |
| `value_counts()` | `group_by(col).aggregate(n=_.count()).order_by(ibis.desc('n'))` |
| `describe()` on a column | `aggregate(mean=, std=, min=, max=)` |
| Boolean filter | `filter()` |
| `sort_values()` | `order_by()` |
| Computed column | `mutate()` |
| `.dt.hour` / `.dt.month` | `t.ts.hour()` / `t.ts.month()` (ibis method names differ from pandas) |
| `case`/`when` mapping | `ibis.case().when(...).else_(...).end()` |
| `corr()` between two columns | `aggregate(r=t.x.corr(t.y))` — verify xorq supports |
| `head(n)` | `limit(n)` |
| `crosstab(a, b)` | `group_by([a, b]).aggregate(n=_.count())` |
| `isnull().sum()` | `aggregate(null_count=col.isnull().sum())` |

### Inherently pandas-specific (won't map)

- `groupby().filter(lambda x: x.count() > N)` — group-size predicate applied back to rows; requires self-join or window in ibis
- `groupby().transform('mean')` — group-mean imputation; maps to window `mutate` but LLM won't get there first
- `.apply()` with arbitrary Python functions
- `pd.melt()` / `pivot_table()` reshaping
- Chunk-by-chunk file loading
- `.iloc`/`.at` positional access
- SMOTE / sklearn preprocessing

---

## Codegen test prompts (ordered easy → hard)

Using `citibike_large` (2.5M Sep 2020 rides, columns: ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual, trip_duration).

| # | Prompt | Expected ibis pattern | Predicted difficulty |
|---|---|---|---|
| 1 | "count the number of null values in each column" | `aggregate(null_count=col.isnull().sum())` per column | easy |
| 2 | "group rides by start_station_name and count how many rides started from each, show top 20" | `group_by().aggregate(n=_.count()).order_by(desc).limit(20)` | easy |
| 3 | "what's the average trip_duration by rideable_type?" | `group_by('rideable_type').aggregate(avg=_.trip_duration.mean())` | easy |
| 4 | "add a column for the hour of day from started_at, then group by hour and count rides" | `mutate(hour=t.started_at.hour()).group_by('hour').aggregate(n=_.count())` | easy |
| 5 | "what share of rides have a null end_station_name?" | `aggregate(pct=_.end_station_name.isnull().sum() / _.count())` | medium — denominator |
| 6 | "compute the mean and standard deviation of trip_duration" | `aggregate(mean=, std=)` | easy |
| 7 | "for each rideable_type, show min, max, mean, and median trip_duration" | multi-agg `group_by().aggregate(min=, max=, mean=, approx_median=)` | medium — median |
| 8 | "filter to rides longer than 60 minutes and count how many there are per rideable_type" | `filter(t.trip_duration > 3600).group_by().aggregate(n=_.count())` | easy |
| 9 | "group by member_casual and rideable_type, give me count and average duration" | `group_by([a, b]).aggregate(n=, avg=)` | easy |
| 10 | "flag rides as 'long' if trip_duration > 3600 and 'short' otherwise — add this as a new column" | `mutate(flag=ibis.case().when(...).else_(...).end())` | easy |
| 11 | "fill missing end_station_name with 'unknown'" | `mutate(col=t.col.fillna('unknown'))` | easy |
| 12 | "for each start station, compute average trip duration — then filter to only stations with more than 100 rides" | requires having-clause or self-join; LLM will try `groupby().filter(lambda)` | hard |
| 13 | "compute the correlation between trip_duration and hour of day" | requires `mutate` first, then `aggregate(r=a.corr(b))` | medium |
| 14 | "show the median trip duration for each hour of the day" | `mutate(hour=).group_by('hour').aggregate(med=_.trip_duration.approx_median())` | medium — median method name |
| 15 | "fill missing end_station_name using the most common station name for that rideable_type" | window aggregate + fillna — hard even in pandas | hard |

---

## Predicted failure modes for xorq codegen

1. **`.describe()` shorthand** — LLM will call `.describe()` on an ibis expression; it doesn't exist. Must decompose into explicit aggregates.
2. **`isnull().mean()`** — natural way to express null share; ibis needs `isnull().sum() / count()`.
3. **Datetime accessor names** — LLM trained on pandas will emit `.dt.hour`; ibis uses `.hour()`.
4. **`groupby().filter(lambda)`** — LLM's default for group-size filtering; requires window or self-join in ibis.
5. **Group-based imputation** — LLM will reach for `.transform('mean')`; ibis needs `col.mean().over(window)`.
6. **`approx_median()` vs `median()`** — ibis uses `approx_median()`; LLM will try `median()` and fail or silently get nothing.
7. **`select(ibis._, ...)`** — ibis wildcard not supported in xorq's vendored ibis; prefer `mutate`.
8. **`import ibis` directly** — must use `import xorq.vendor.ibis as ibis`.
