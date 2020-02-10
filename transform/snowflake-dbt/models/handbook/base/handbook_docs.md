{% docs handbook_merge_requests %}

This data is gathered by hitting the GitLab API for project 7764, which includes the handbook.  

All merge request data is pulled for this project, and becomes part of the source for handbook_merge_requests.

The data is filtered to only include those merge requests that have a file that is part of the handbook.

The `handbook_file_classification_mapping.csv` is loaded and used to classify each handbook MR into one or more departments or divisions for easy analysis of how often the handbook is being changed for each department of interest.  

This data is also joined to the gitlab db data to get current merge request statuses.

{% enddocs %}