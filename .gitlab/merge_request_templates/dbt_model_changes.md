## Issue

(Describe the Issue)


## Solution

(Describe the solution in summary first and in technical detail second)

## Checklist

(If making changes to dbt models)

[] Do you follow the coding conventions laid out in the [style guide](https://gitlab.com/meltano/meltano#dbt-coding-conventions)? 
[] Model-specific attributes (like sort/dist keys) should be specified in the model.
[] Only base mdoels are used to reference source tables/views.
[] All {{ ref('...') }} statements should be placed in CTEs at the top of the file.
[] Field names should all be lowercased.
[] Function names should all be capitalized.
[] Every model should be tested in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested (if applicable)
[] The output of dbt test should be pasted into MRs

## Related Links

(Please include links to any related MRs and/or issues.)
