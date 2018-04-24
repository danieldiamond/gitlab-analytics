# GitLab Data and Analytics

This repository is for the storage of Looker view, model, and dashboard files. Use the issue tracker to request new data visualizations, suggest improvements to exisiting ones, report errors in underlying data, or request new data sources and features.

# Contributing to BizOps

We welcome contributions and improvements, please see the [contribution guidelines](CONTRIBUTING.md)

## Getting Started

The instructions below will get help you understand GitLab's data infrastructure and tools used to produce GitLab's internal data products and insights.

## Prerequisites

Users may request access to using GitLab's internal data analytic tools. Depending on the needs of a user, a user may need to request the following items to gain access to the data analytic tools.

##### Required

```
 - Request access to the Looker platform
```

##### Optional

```
-
```

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

# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE.mdf) file.
