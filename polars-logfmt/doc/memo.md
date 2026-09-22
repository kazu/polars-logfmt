Column への追記方法
```rust
// create column (name stringは適宜)
let mut col = Column::new(PlSmallStr::from("x"), vec![1i64, 2i64]);

// 1) Series を作って直接追加（into_materialized_series を使う）
let s = Series::new("x", &[3i64]);
col.into_materialized_series().append(&s)?;

// 2) Column として追加（append_owned を使う）
let c2: Column = Series::new("x", &[4i64]).into();
col.append_owned(c2)?;
```


```rust
            // Create a single-value Series from ParsedValue and append to column.
            let s = match pv {
                ParsedValue::String(ref v) => polars::prelude::Series::new(key.into(), vec![v.clone()]),
                ParsedValue::Integer(i) => polars::prelude::Series::new(key.into(), &[i]),
                ParsedValue::Float(f) => polars::prelude::Series::new(key.into(), &[f]),
                ParsedValue::Boolean(b) => polars::prelude::Series::new(key.into(), &[b]),
                ParsedValue::DateTime(dt) => {
                    // store datetime as RFC3339 string for now (owned)
                    polars::prelude::Series::new(key.into(), vec![dt.to_rfc3339()])
                }
            };

            if let Some(col_mut) = columns.iter_mut().find(|c| c.name() == key) {
                // materialize and append
                col_mut.into_materialized_series().append(&s).unwrap();
            } else {
                // new Column from Series
                columns.push(s.into());
            }
```



```
            self.columns_as_line
                .entry(key.to_string())
                .or_insert_with(Vec::<ParsedValue>::new)
                .push(pv);

            // Try to reuse existing Vec (no allocation). Only allocate key String when missing.
            // if let Some(vec) = self.columns_as_line.get_mut(key) {
            //     vec.push(pv);
            // } else {
            //     self.columns_as_line.insert(key.to_owned(), vec![pv]);
            // }
```

1s 程度


```rust
let cvalues: Vec<String> = values.iter().map(|x| x.as_string()).collect();
```

これを

```rust
                        let mut cvalues = Vec::with_capacity(values.len());
                        for pv in values.into_iter() {
                            cvalues.push(pv.as_string());
                        }
```

```rust

let cvalues: Vec<String> = values.into_iter().map(|x| x.as_string()).collect();

```

こうなってもパフォーマンスは変わらなくないですか？

Column::new(key.as_str().into(), cvalues)
Column::new(key.clone(), cvalues)


```
    let timestamps: Vec<i64> = date_times
        .iter()
        .map(|dt| dt.timestamp_micros())
        .collect();

    // 3. Series を作成し、Datetime 型へキャスト
    let series = Series::new("timestamp_col".into(), timestamps)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    // 4. Column として扱う場合
    let column = Column::Series(series);

    println!("{:?}", column);
    Ok(())
```
let start_time = std::time::Instant::now();
                tracing::trace!(elapsed = %humantime::format_duration(start_acc_time.elapsed()),
                "finish frame accumulate_dataframes_vertical");