{% docs gitlab_dotcom_gitlab_subscriptions %}

Base model for Gitlab.com gitlab_subscriptions. These are the plan subscriptions for the gitlab.com product, as opposed to the `subscriptions` table (no prefix) which deals with subscribing to issues and merge requests.

{% enddocs %}


{% docs visibility_documentation %}
This content will be masked for privacy in one of the following conditions:
 * If this is an issue, and the issue is set to `confidential`
 * If the namespace or project visibility level is set to "internal" (`visibility_level` = 10) or "private" (`visibility_level` = 0).
    * The visibility values can be validated by going to the [project navigation](https://gitlab.com/explore) and using the keyboard shortcut "pb" to show how the front-end queries for visibility.
 * Public projects are defined with a `visibility_level` of 20   
 * In all the above cases,  the content will *not* be masked if the namespace_id is in: 
   * 6543: gitlab-com
   * 9970: gitlab-org
   * 4347861: gitlab-data  
     
{% enddocs %}