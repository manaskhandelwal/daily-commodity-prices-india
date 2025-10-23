Daily market prices of agricultural commodities across India from **2001-2025**. Contains **75+ million** records covering **374 unique commodities** and **1,504 varieties** from various mandis (wholesale markets). Commodity Like: Vegetables, Fruits, Grains, Spices, etc.

Cleaned, deduplicated, and sorted by date and commodity for analysis.

### Column Schema

| Column         | Description                                                                         | Description |
| -------------- | ----------------------------------------------------------------------------------- | ----------- |
| State          | Name of the Indian state where the market is located                                | `province`  |
| District       | Name of the district within the state where the market is located                   | `city`      |
| Market         | Name of the specific market (mandi) where the commodity is traded                   | `string`    |
| Commodity      | Name of the agricultural commodity being traded                                     | `string`    |
| Variety        | Specific variety or type of the commodity                                           | `string`    |
| Grade          | Quality grade of the commodity (e.g., FAQ, Medium, Good)                            | `string`    |
| Arrival_Date   | The date of the price recording, in unambiguous ISO 8601 format (YYYY-MM-DD).       | `datetime`  |
| Min_Price      | Minimum price of the commodity on the given date (in INR per quintal)               | `decimal`   |
| Max_Price      | Maximum price of the commodity on the given date (in INR per quintal)               | `decimal`   |
| Modal_Price    | Modal (most frequent) price of the commodity on the given date (in INR per quintal) | `decimal`   |
| Commodity_Code | Unique code identifier for the commodity                                            | `numeric`   |

---

Data sourced from the Government of India's Open Data Platform.

**License:**
Government Open Data License - India (GODL-India)
https://www.data.gov.in/Godl
