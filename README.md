# GitLab Data and Analytics

This repository is for the storage of Looker view, model, and dashboard files. Use the issue tracker to request new data visualizations, suggest improvements to exisiting ones, report errors in underlying data, or request new data sources and features.

# Contributing to BizOps

We welcome contributions and improvements, please see the [contribution guidelines](CONTRIBUTING.md)

## Getting Started

The information below will get help you understand GitLab's data infrastructure and tools used to produce GitLab's internal data products and insights.

## Prerequisites

All GitLab employees have access to our Looker instance. We do not use Google Authentication so each person will need to request access. This can be done in bulk by making an issue in this project and including the following:

   * Email addresses to be added
   * Functional Group (Sales, Finance, Engineering, etc.) for each user
   * Whether Developer or View-Only Access is required
   * Assign the issue to @tayloramurphy
   * Add the Looker label
​

## Department Spaces, Dashboards & Looks
							
[Engineering](https://gitlab.looker.com/spaces/24)|[Finance ](https://gitlab.looker.com/spaces/16)|[Investor ](https://gitlab.looker.com/spaces/20)|[Marketing ]|[PeopleOps ]|[Product ](https://gitlab.looker.com/spaces/18)|[Sales ](https://gitlab.looker.com/spaces/28)|[Support ]
-- | -- | -- | -- | -- | -- | -- | -- |
[Dashboard 1]()|[Dashboard 1]()|[Dashboard 1]()|[Dashboard 1]()|[Dashboard 1]()|[Dashboard 1]()|[Dashboard 1]()|[Dashboard 1]()
[Dashboard 2]()|[Dashboard 2]()|[Dashboard 2]()|[Dashboard 2]()|[Dashboard 2]()|[Dashboard 2]()|[Dashboard 2]()|[Dashboard 2]()
||||||||
[Look 1]()|[Look 1]()|[Look 1]()|[Look 1]()|[Look 1]()|[Look 1]()|[Look 1]()|[Look 1]()
[Look 2]()|[Look 2]()|[Look 2]()|[Look 2]()|[Look 2]()|[Look 2]()|[Look 2]()|[Look 2]()
||||||||

### GitLab Looker Help
- Slack - #analytics


## GitLab Internal Analytics Architecture

![GitLab Internal Analytics Architecture](https://gitlab.com/bizops/bizops/raw/master/img/WIP_%20GitLab_Analytics_Architecture.jpg)

## Core Data and Analytics Toolset

### Looker

[Looker](https://docs.looker.com) is a business intelligence software and big data analytics platform that helps users explore, analyze and share real-time business analytics easily.

#### Core Looker Definitions & Structure

**LookML** - is a Looker proprietary language for describing dimensions, aggregates, calculations and data relationships in a SQL database. The Looker app uses a model written in LookML to construct SQL queries against a particular database.

**LookML Project** - A LookML project is a collection of LookML files that describe a set of related models, explores, views, and LookML dashboards.

By convention, LookML code is segregated into three types of files: model files, view files, and dashboard files. In new LookML, these fields have the following extensions: .model.lkml, .view.lkml, and .dashboard.lookml, respectively.

**Model** - A model is a customized portal into the database, designed to provide intuitive data exploration for specific business users. Multiple models can exist for the same database connection in a single LookML project. Each model can expose different data to different users. For example, sales agents need different data than company executives, and so you would probably develop two models to offer views of the database appropriate for each user. A model, in turn, is essentially metadata about the source database.

**View** - A view declaration defines a list of fields (dimensions or measures) and their linkage to an underlying table or derived table. In LookML a view typically references an underlying database table, but it can also represent a derived table.

A view may join to other views. The relationship between views is typically defined as part of a explore declaration in a model file.

**Explore** - An explore is a view that users can query. You can think of the explore as a starting point for a query, or in SQL terms, as the FROM in a SQL statement. An explore declaration specifies the join relationships to other views.

**Derived Table** - A derived table is a table comprised of values from other tables, which is accessed as though it were a physical table with its own set of columns. A derived table is exposed as its own view using the derived_table parameter, and defines dimensions and measures in the same manner as conventional views. Users can think of a derived table similar to a **view**, not to be confused with a Looker view, in a database.

**Dimension Group** - The dimension_group parameter is used to create a set of time-based dimensions all at once. For example, you could easily create a date, week, and month dimension based on a single timestamp column.

### Google Cloud SQL

[Cloud SQL](https://cloud.google.com/sql/docs/postgres/) - is a fully-managed database service that makes it easy to set up, maintain, manage, and administer GitLab's PostgreSQL relational databases on Google Cloud Platform.

### JupyterHub

[JupyterHub](https://jupyterhub.readthedocs.io/en/latest/) - a multi-user Hub that spawns, manages, and proxies multiple instances of the single-user Jupyter notebook server. The Jupyter Notebook is an open-source web application that allows you to create and share documents that contain live code, equations, visualizations and explanatory text.

## Guidelines

For effective development and consistency, general guidelines have been added below for reference.

### General Guidelines

- Percentages should appear as integer values
  - ```ex: 30% ```

### Dashboards
- Chart Titles should exist to explain the chart
- Fields should be define based on the business, not how it is labeled in a database table
  - ```ex: TCV or Total Contract Value, not tcv_value```

## Best Practices

### dbt

- Watch [this video (GitLab internal)](https://drive.google.com/open?id=1ZuieqqejDd2HkvhEZeOPd6f2Vd5JWyUn) on how to use dbt
- Use dbt for as much modeling as possible - see this [blog post](https://blog.fishtownanalytics.com/how-do-you-decide-what-to-model-in-dbt-vs-lookml-dca4c79e2304) from Fishtown Analytics.

### Looker

- Make mulitple small explores intead of one big one
- Design each explore to answer a specific set of questions. Single-view explores are fine!
- 5 to 6 view joins should be the limit. Otherwise, the underlying data may need to be modeled more effectively
- Only include specific fields that answer the question needed for analysis. Do not include fields that won't be used
- Don't use full outer joins
- Don’t use many-to-many joins

#### Spaces
There are two primary spaces from a user perspective: "Shared" and your personal space named after your user. Access these via the "Browse" dropdown. It is strongly recommended that you initially develop all looks and dashboards in your personal space first and then move or copy them to a relevant sub-space within the "Shared" space when they're ready.

The Shared space should be for looks and dashboards that you want for your team or for the whole company. Do not create dashboards or looks within the top-level Shared space - alwyays put them in a subspace of Shared. Think of "Shared" as the root folder and each sub-space as a folder within root. Each subspace can have as many sub-spaces as a team wants. Initially, there will be a subspace within shared for each functional group and we can iterate from there.

## Resources
- [Looker Documentation](https://docs.looker.com)
- [Fishtown Analytics Blog](https://blog.fishtownanalytics.com)
- [Data Science Roundup Newsletter](http://roundup.fishtownanalytics.com/)
- [Mode Analytics Blog](https://blog.modeanalytics.com/)
- [Looker Blog](https://looker.com/blog)
- [Periscope Data Blog](https://www.periscopedata.com/blog)
- [Yhat Blog](http://blog.yhat.com/)
- [Wes McKinney Blog](http://wesmckinney.com/archives.html)


# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE.mdf) file.
