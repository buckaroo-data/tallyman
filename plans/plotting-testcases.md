# Plotting test cases (Vega-Lite, from the wild)

Generated 2026-05-30. Sourced from the Vega-Lite example gallery (vega/vega-lite `examples/specs`,
632 specs) and adapted to the demo datasets' real columns. These extend the plotting evaluation beyond
the 5 archetypes already exercised (single bar, line/multi-line, histogram, scatter, rect-heatmap).

**How to use as a test case.** Give a model the *intent* + the entry's column list and ask it to produce a
Vega-Lite v5 spec for `catalog_chart`; then validate: (1) parses as JSON, (2) valid VL structure
(mark/layer/facet/repeat + encoding), (3) every encoded `field` exists in the entry's built schema,
(4) no inlined `data.values` (the companion serves the parquet), (5) right archetype for the ask. The
reference spec below is one correct answer (column names match the entry).

Datasets/entries referenced: `diamonds` (carat, cut, color, clarity, depth, table, price, x, y, z),
`penguins` (species, island, bill_length_mm, bill_depth_mm, flipper_length_mm, body_mass_g, sex),
`titanic` (survived, pclass, sex, age, fare, class, embarked, alive), `wine_all` (wine_type, alcohol, pH,
density, "fixed acidity", quality, …), `seattle_weather` (date, precipitation, temp_max, temp_min, wind,
weather), `mpg` (mpg, cylinders, horsepower, weight, acceleration, model_year, origin), `stock_returns`
(symbol, date_parsed, price, monthly_return, ma_3m).

---

## 1. Grouped bar (xOffset) — diamonds  ·  gallery: `bar_grouped`
**Intent:** "mean price by cut, grouped by color, side-by-side bars."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":"bar",
 "encoding":{"x":{"field":"cut","type":"ordinal","sort":["Fair","Good","Very Good","Premium","Ideal"]},
   "xOffset":{"field":"color","type":"nominal"},
   "y":{"aggregate":"mean","field":"price","type":"quantitative"},
   "color":{"field":"color","type":"nominal"}}}
```

## 2. Normalized stacked bar (100%) — diamonds  ·  gallery: `stacked_bar_normalize`
**Intent:** "clarity composition within each cut as a percentage."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":"bar",
 "encoding":{"x":{"field":"cut","type":"ordinal","sort":["Fair","Good","Very Good","Premium","Ideal"]},
   "y":{"aggregate":"count","type":"quantitative","stack":"normalize","title":"share"},
   "color":{"field":"clarity","type":"nominal"}}}
```

## 3. Box plot (2D) — penguins  ·  gallery: `boxplot_2D_vertical`
**Intent:** "distribution of body mass for each species as box plots."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "mark":{"type":"boxplot","extent":"min-max"},
 "encoding":{"x":{"field":"species","type":"nominal"},
   "y":{"field":"body_mass_g","type":"quantitative","scale":{"zero":false}},
   "color":{"field":"species","type":"nominal"}}}
```

## 4. Grouped bar + error bars — wine_all  ·  gallery: `bar_grouped_errorbar`
**Intent:** "mean alcohol by wine type with error bars." (Tests layering bar + errorbar on an aggregate.)
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "encoding":{"x":{"field":"wine_type","type":"nominal"}},
 "layer":[
   {"mark":"bar","encoding":{"y":{"aggregate":"mean","field":"alcohol","type":"quantitative"},
     "color":{"field":"wine_type","type":"nominal"}}},
   {"mark":"errorbar","encoding":{"y":{"field":"alcohol","type":"quantitative"}}}]}
```

## 5. Scatter + regression line + R² — diamonds  ·  gallery: `layer_point_line_regression`
**Intent:** "price vs carat with a fitted regression line and the R² printed on the chart."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "layer":[
  {"mark":{"type":"point","filled":true,"opacity":0.2},
   "encoding":{"x":{"field":"carat","type":"quantitative"},"y":{"field":"price","type":"quantitative"}}},
  {"mark":{"type":"line","color":"firebrick"},
   "transform":[{"regression":"price","on":"carat"}],
   "encoding":{"x":{"field":"carat","type":"quantitative"},"y":{"field":"price","type":"quantitative"}}},
  {"transform":[{"regression":"price","on":"carat","params":true},
                {"calculate":"'R²: '+format(datum.rSquared,'.2f')","as":"R2"}],
   "mark":{"type":"text","color":"firebrick","x":"width","align":"right","y":-5},
   "encoding":{"text":{"type":"nominal","field":"R2"}}}]}
```

## 6. Scatter + LOESS smooth — mpg  ·  gallery: `layer_point_line_loess`
**Intent:** "fuel economy vs horsepower with a loess trend line."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "layer":[
  {"mark":{"type":"point","filled":true},
   "encoding":{"x":{"field":"horsepower","type":"quantitative"},"y":{"field":"mpg","type":"quantitative"}}},
  {"mark":{"type":"line","color":"firebrick"},
   "transform":[{"loess":"mpg","on":"horsepower"}],
   "encoding":{"x":{"field":"horsepower","type":"quantitative"},"y":{"field":"mpg","type":"quantitative"}}}]}
```

## 7. Density plot (overlaid) — penguins  ·  gallery: `area_density`
**Intent:** "kernel density of flipper length, one curve per species."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "mark":{"type":"area","opacity":0.5},
 "transform":[{"density":"flipper_length_mm","groupby":["species"]}],
 "encoding":{"x":{"field":"value","type":"quantitative","title":"flipper_length_mm"},
   "y":{"field":"density","type":"quantitative","stack":null},
   "color":{"field":"species","type":"nominal"}}}
```

## 8. Bubble chart (size encoding) — penguins  ·  gallery: `circle` + `_size`
**Intent:** "bill length vs bill depth, point size = body mass, colored by species."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "mark":{"type":"circle","opacity":0.7},
 "encoding":{"x":{"field":"bill_length_mm","type":"quantitative","scale":{"zero":false}},
   "y":{"field":"bill_depth_mm","type":"quantitative","scale":{"zero":false}},
   "size":{"field":"body_mass_g","type":"quantitative"},
   "color":{"field":"species","type":"nominal"}}}
```

## 9. 2D binned heatmap (2D histogram) — diamonds  ·  gallery: `rect` binned
**Intent:** "density of diamonds across carat and price as a binned heatmap."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":"rect",
 "encoding":{"x":{"field":"carat","bin":{"maxbins":40},"type":"quantitative"},
   "y":{"field":"price","bin":{"maxbins":40},"type":"quantitative"},
   "color":{"aggregate":"count","type":"quantitative","scale":{"scheme":"viridis"}}}}
```

## 10. Faceted scatter — penguins  ·  gallery: `trellis` / facet
**Intent:** "bill length vs flipper length, one panel per island."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":"point",
 "encoding":{"x":{"field":"bill_length_mm","type":"quantitative","scale":{"zero":false}},
   "y":{"field":"flipper_length_mm","type":"quantitative","scale":{"zero":false}},
   "color":{"field":"species","type":"nominal"},
   "column":{"field":"island","type":"nominal"}}}
```

## 11. Scatterplot matrix (SPLOM) — penguins  ·  gallery: `interactive_splom` / `repeat`
**Intent:** "a scatterplot matrix of the four body measurements, colored by species."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "repeat":{"row":["bill_length_mm","bill_depth_mm","flipper_length_mm","body_mass_g"],
   "column":["body_mass_g","flipper_length_mm","bill_depth_mm","bill_length_mm"]},
 "spec":{"mark":"point","encoding":{
   "x":{"field":{"repeat":"column"},"type":"quantitative","scale":{"zero":false}},
   "y":{"field":{"repeat":"row"},"type":"quantitative","scale":{"zero":false}},
   "color":{"field":"species","type":"nominal"}}}}
```

## 12. Dual-axis bar + line — seattle_weather  ·  gallery: `layer_bar_line`
**Intent:** "by month, bars of avg precipitation and a line of avg max temperature on a second axis."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "encoding":{"x":{"field":"date","timeUnit":"month","type":"ordinal","title":"month"}},
 "layer":[
  {"mark":"bar","encoding":{"y":{"aggregate":"mean","field":"precipitation","type":"quantitative"}}},
  {"mark":{"type":"line","color":"firebrick","point":true},
   "encoding":{"y":{"aggregate":"mean","field":"temp_max","type":"quantitative"}}}],
 "resolve":{"scale":{"y":"independent"}}}
```

## 13. Cumulative line via window transform — stock_returns  ·  gallery: `area_cumulative_freq` / window
**Intent:** "cumulative sum of monthly return over time, one line per symbol."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "transform":[{"sort":[{"field":"date_parsed"}],
   "window":[{"op":"sum","field":"monthly_return","as":"cum_return"}],"groupby":["symbol"]}],
 "mark":"line",
 "encoding":{"x":{"field":"date_parsed","type":"temporal"},
   "y":{"field":"cum_return","type":"quantitative"},
   "color":{"field":"symbol","type":"nominal"}}}
```

## 14. Donut / arc — titanic  ·  gallery: `arc_donut`
**Intent:** "share of passengers by class as a donut."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "mark":{"type":"arc","innerRadius":60},
 "encoding":{"theta":{"aggregate":"count","type":"quantitative"},
   "color":{"field":"class","type":"nominal"}}}
```

## 15. Histogram + mean rule overlay — diamonds  ·  gallery: `layer` (histogram + rule)
**Intent:** "price distribution with a vertical line at the mean price."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "layer":[
  {"mark":"bar","encoding":{"x":{"field":"price","bin":{"maxbins":40},"type":"quantitative"},
    "y":{"aggregate":"count","type":"quantitative"}}},
  {"mark":{"type":"rule","color":"red","size":2},
   "encoding":{"x":{"aggregate":"mean","field":"price","type":"quantitative"}}}]}
```

## 16. Heatmap with text labels — titanic  ·  gallery: `layer` rect + text
**Intent:** "survival rate over the sex × class grid, with the rate printed in each cell."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "encoding":{"x":{"field":"pclass","type":"ordinal"},"y":{"field":"sex","type":"nominal"}},
 "layer":[
  {"mark":"rect","encoding":{"color":{"aggregate":"mean","field":"survived","type":"quantitative",
    "scale":{"scheme":"redyellowgreen","domain":[0,1]}}}},
  {"mark":"text","encoding":{"text":{"aggregate":"mean","field":"survived","type":"quantitative",
    "format":".0%"}}}]}
```

## 17. Strip / tick plot — titanic  ·  gallery: `tick`
**Intent:** "fare for each passenger as a strip plot, one row per class."
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":"tick",
 "encoding":{"x":{"field":"fare","type":"quantitative"},
   "y":{"field":"class","type":"nominal"},
   "color":{"field":"class","type":"nominal"}}}
```

## 18. Candlestick — stocks  ·  gallery: `layer_candlestick`  ·  STRETCH / negative test
**Intent:** "a candlestick chart of the stock prices." **Catch:** our `stocks`/`stock_returns` data has only a
monthly *close* (`price`) — no open/high/low. A correct response should either refuse, or first derive an OHLC
entry (it cannot from monthly closes alone). This tests whether the model fabricates open/high/low fields that
don't exist (a chart referencing missing columns renders empty). Reference shape (requires an `ohlc` entry):
```json
{"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
 "encoding":{"x":{"field":"date","type":"temporal"},
   "color":{"condition":{"test":"datum.open < datum.close","value":"#06982d"},"value":"#ae1325"}},
 "layer":[
  {"mark":"rule","encoding":{"y":{"field":"low","type":"quantitative","scale":{"zero":false}},"y2":{"field":"high"}}},
  {"mark":"bar","encoding":{"y":{"field":"open","type":"quantitative"},"y2":{"field":"close"}}}]}
```

---

## Coverage map (archetype → test case)
- already exercised: single/grouped bar (1), single line, multi-line + layered overlay, histogram (15), scatter (5/6/8), rect heatmap (16).
- new here: stacked-normalized bar (2), boxplot (3), error bars (4), error band, regression overlay (5), loess (6), density (7), bubble/size (8), 2D binned heatmap (9), facet (10), SPLOM/repeat (11), dual-axis (12), window/cumulative (13), arc/donut (14), text-label layer (15/16), strip/tick (17), candlestick negative test (18).
- not yet covered (candidates for a future pass): geographic/`geoshape` choropleth (needs topojson — out of scope for these tabular datasets), interactive selection/brush, streamgraph, waterfall, radial/arc histogram.
