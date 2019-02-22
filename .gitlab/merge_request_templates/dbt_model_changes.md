## Issue

Describe the Issue


## Solution

Describe the solution.

## Checklist

- [ ] This MR follows the coding conventions laid out in the [style guide](https://gitlab.com/meltano/meltano#dbt-coding-conventions).
- [ ] Model-specific attributes (like sort/dist keys) should be specified in the model.
- [ ] Only base models are used to reference source tables/views.
- [ ] All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file.
- [ ] Field names should all be lowercased.
- [ ] Function names should all be capitalized.
- [ ] Every model should be tested AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable ([Docs](https://docs.getdbt.com/docs/testing-and-documentation)).
- [ ] Any new macros have been documented in the macro README.
- [ ] The output of dbt test should be pasted into MRs.

## Related Links

Please include links to any related MRs and/or issues.

## Testing

<details>
<summary> dbt test results </summary>

<pre><code>

Paste the results of dbt test here, including the command.

</code></pre>
</details>
