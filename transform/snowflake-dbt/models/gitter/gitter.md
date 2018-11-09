{% docs gitter_client_access %}

Whenever a user hits a Gitter server, we track who it was and what type of client(destkop, mobile) they are using

 - https://gitlab.com/gitlab-org/gitter/webapp/blob/d39b24c6fa894a608a3c0138df3165594cadf78c/server/utils/client-usage-stats.js#L33-39

Data is from MongoDB Cube, http://cube-01.prod.gitter, http://cube-02.prod.gitter

{% enddocs %}


{% docs gitter_active_users_client_access %}

Used for getting active users on Gitter across time (day, week, month, year).

This table holds every date/user and the days since they last accessed Gitter

{% enddocs %}
