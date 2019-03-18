{% docs tappg_model %}
This model is this data from tap-postgres. It is backfilled by the version form. It is unioned in the following model. There is a roughly 3 week overlap, so the date filter is flexible. 
{% enddocs %}

{% docs version_model %}
This model is this data gcloud EL'ed through Stitch. It is no longer updated. It is unioned in the following model. There is a roughly 3 week overlap, so the date filter is flexible. 
{% enddocs %}
