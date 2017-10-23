## Bizops

Finance has the company wide responsibility for analytics.

We want a single data warehouse and a central data model. We bring all relevant data to single storage place. Make it platform agnostic, so for example do not build it into Salesforce. We want to build data model to be used consistently across tools and teams. For example something as simple as unique customer ID, product or feature names/codes.

### Cross department metrics

Cross department metrics are essential to measure our [journeys](/handbook/journeys).

### Operating metrics

More information about our metrics definitions and review process can be found on the [operating metrics page](/handbook/finance/operating-metrics/).

### Data sources

1. Salesforce
1. Zuora
1. Marketo
1. Zendesk
1. NetSuite
1. Mailchimp
1. Google Analytics
1. Discover.org
1. Lever
1. GitLab version check
1. GitLab usage ping
1. [GitLab.com](https://about.gitlab.com/handbook/engineering/workflow/#getting-data-about-gitlabcom)

### Tools

We want the tools to be open source so we can ship this as a product. Also see the "Complete BizOps" Google Doc. We'll use Singer, PostgreSQL, and Superset.

1. Ingestion/ETL: The open source [Singer](https://www.singer.io/) from [StitchData](https://www.stitchdata.com/). This competes with the proprietary options from them and [Fivetran](https://www.fivetran.com/), [Alooma](https://www.alooma.com/), [Segment and Looker](https://looker.com/blog/segment-and-looker), and [Snaplogic](https://www.snaplogic.com/).
1. Warehouse: [PostgeSQL](https://www.postgresql.org/), maybe later with [a column extension](https://github.com/citusdata/cstore_fdw). If people need SaaS BigQuery is nice option and [Druid](http://druid.io/) seems like a good pure column oriented database.
1. Display/analytics: [Superset](https://github.com/airbnb/superset) (Apache open source, [most](https://github.com/apache/incubator-superset/pulse/monthly) [changed](https://github.com/metabase/metabase/pulse/monthly) [repository](https://github.com/getredash/redash/pulse/monthly)) instead of the open source alternative [Metabase](https://github.com/metabase/metabase) which is Clojure based and runs on the JVM or [Redash](https://redash.io/). Proprietary alternates are Looker and Tableau.

### Metrics

**Annual Recurring Revenue (ARR)** - Recurring revenue recognized in current month multiplied by 12. (Source data from Zuora)

**Average Sales Price (ASP)** - IACV per won

**Bookings Total Contract Value (TCV)** - All bookings in period (including multiyear); bookings is equal to billings with standard payment terms. (Source data from Salesforce reconciled to Zuora)

**Bookings Annual Contract Value (ACV)** - Current Period subscription bookings which will result in revenue over next 12 months. (Source data from Salesforce reconciled to Zuora)

**Bookings Incremental Annual Contract Value (IACV)** - Value of new bookings from new and existing customers that will result in revenue of next 12 months. Also equals ACV less renewals. (Source data from Salesforce reconciled to Zuora)

**Churn, Net** - Current period revenue from customers present 12 months prior divided by revenue from 12 months prior. (Source data from excel; Zuora expected 2017-07-01)

**Churn, Gross (Dollar weighted)** - Remaining cohorts from Actual Subscription customers active as of 12 months ago multiplied by revenue from 12 months ago divided by actual Subscription customers as of date 12 months prior multiplied by revenue from 12 months ago. (Source data from excel; Zuora expected 2017-07-01)

**Customer Acquisition Cost (CAC)** - Total Sales & Marketing Expense/Number of New Customers Acquired (Source Zuora and Salesforce)

**Rep Productivity** - Current Month [Metric] /# of Reps on board for at least 90 days prior to start of period. (Source data bamboo HR)

**Magic Number** - IACV for trailing three months/Sales & Marketing Spend over trailing six months. (Source data Salesforce/Zuora and Netsuite)

**MQL** - Marketing Qualified Lead (Source data Salesforce)

**SQL** - Sales Qualified Lead (Source data Salesforce)

**Cost per MQL** - Marketing expense divided by # MQLs (Source data Salesforce and Netsuite)

**Sales efficiency ratio** - IACV / sales (and marketing) spend

**Marketing efficiency ratio** - IACV / marketing spend

### Dockerfile
The image combines [Apache Superset](https://superset.incubator.apache.org/index.html) with a [PostgreSQL](https://www.postgresql.org/) database. It creates an image pre-configured to use a local PostgreSQL database and loads the sample data and reports. The setup.py file launches the database as well as starts the Superset service on port 8080 using [Gunicorn](http://gunicorn.org/).


### To launch the container:

1. Clone the repo and cd into it.
2. Edit the config/bizops.conf file to enter in a username, first name, last name, email and password for the Superset administrator.
3. Optionally, you can edit the user and password in the Dockerfile on line 35 to change the defaule postgres user/password. If you do this, you'll also need to update the SQLAlchemy url in the /config/superset_config.py file on line 21
4. Build the image with `docker build --rm=true -t bizops .`.
5. Run the image with `docker run -p 80:8088 bizops`.
6. Go to [http://localhost](http://localhost) and log in using the credentials you entered in step 2. 


### License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.