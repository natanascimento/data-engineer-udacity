# Data Dictionary 

## **DIM_COMPANY_ADDRESS**

| column_name  | description  | data_type |
|---|---|---|
| symbol | Symbol of the company. This is FK to the DIM_COMPANY table | string |
| street | Street of the company | string |
| city |  City of the company | string |
| state |  State of the company | string |
| country |  Country of the company | string |

## **DIM_COMPANY**

| column_name  | description  | data_type |
|---|---|---|
| symbol  | Symbol of the company. This is PK |  string |
| asset_type |  Asset type of the company |  string |
| name | Name of the company  | string |
| description |  Description of the company | string  |
| cik | Central Index Key  |  string |
| exchange  | Exchange when the company was listed | string  |
| currency  | Currency of the company|  string |
| country  | Country of the company|  string |
| sector  | Sector of the company|  string |
| industry  | Industry of the company |  string |


## **FACT_COMPANY_METRICS**

| column_name   | data_type |
|---|---|
| symbol | string |
| FiscalYearEnd | string |
| LatestQuarter | string |
| MarketCapitalization | string |
| EBITDA | string |
| PERatio | string |
| PEGRatio | string |
| BookValue | string |
| DividendPerShare | string |
| DividendYield | string |
| EPS | string |
| RevenuePerShareTTM | string |
| ProfitMargin | string |
| OperatingMarginTTM | string |
| ReturnOnAssetsTTM | string |
| ReturnOnEquityTTM | string |
| RevenueTTM | string |
| GrossProfitTTM | string |
| DilutedEPSTTM | string |
| QuarterlyEarningsGrowthYOY | string |
| QuarterlyRevenueGrowthYOY | string |
| AnalystTargetPrice | string |
| TrailingPE | string |
| ForwardPE | string |
| PriceToSalesRatioTTM | string |
| PriceToBookRatio | string |
| EVToRevenue | string |
| EVToEBITDA | string |
| Beta | string |
| 52WeekHigh | string |
| 52WeekLow | string |
| 50DayMovingAverage | string |
| 200DayMovingAverage | string |
| SharesOutstanding | string |
| DividendDate | string |
| ExDividendDate | string |

## **FACT_EARNINGS**

| column_name  | data_type |
|---|---|
| symbol | string |
| fiscalDateEnding | string |
| reportedDate | string |
| reportedEPS | string |
| estimatedEPS | string |
| surprise | string |
| surprisePercentage | string |


## **FACT_STOCK_MARKET**

| column_name | data_type |
|---|---|
| symbol | string |
| date | string |
| 1_open | string |
| 2_high | string |
| 3_low | string |
| 4_close | string |
| 5_adjusted_close | string |
| 6_volume | string |
| 7_dividend_amount | string |
| 8_split_coefficient | string |