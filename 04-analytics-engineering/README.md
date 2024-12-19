# Analytics Engineering

## What Is Analytics Engineering?

- Data engineer: Prepares and maintain the infrastructure needed by the data team;
- Data analyst: Uses data to answer questions and solve problem;
- Analytics engineer: Introduces good software engineering practices to the efforts of the data analysts and data scientists.

### Tooling

```mermaid
  graph LR;
      A[Data Loading]-->B[Data Storing];
      B-->C[Data Modelling];
      C-->D[Data presentation];
```

- _Data loading_: Fivetran, stitch
- _Data storing_: Cloud data warehouses such as Snowflake, Bigquery and Redshift
- _Data modelling_: dbt, dataform
- _Data presentation_: Google data studio, looker, Tableau

## What Is Data Modelling

### ETL vs ELT

ETL: Extract, Transform, Load: Transform is done before loading into the Data Warehouse

- More stable and compliant data analysis
- Higher storage and compute cost

ELT: Extract, Load, Transform: Transform is done __in__ the Data Warehouse.

- Faster and more flexible data analysis
- Lower cost and lower maintenance.

### Kimball's Dimensional Modeling

__Objective__:

- Deliver dtaa understandable to the business user
- Deliver fast query performance

__Approach__: Prioritize user understandability over non redundant data (3NF).

### Elements of Dimensional Modeling

__Facts tables__:

- Measurements, metrics, fact
- Correspond to business process
- "Verbs"
- Ex: Sales, Orders

__Dimensions tables__:

- Correspond to a business entity
- Provides context to a business process
- "Nouns"
- Ex: Customer, Products

### Architecture of Dimensional Modeling

#### Stage Area

- Raw data
- Not meant to be exposed to everyone

#### Processing area

- From raw data to data models
- Focus on efficiency
- Ensuring standard

#### Presentation area

- Final presentation of the data
- Expose to business stakeholders

The analogy: a restaurant.

- Stage: Ingredients
- Processing: Kitchen. Only those who know how to transform ingredients to courses can touch the data.
- Presentation: Dining hall.
