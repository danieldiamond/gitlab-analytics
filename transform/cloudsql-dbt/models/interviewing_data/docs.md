{% docs interviewing_data_hired_candidates %}
This base model take the raw data which has one interview per row and denests it to be one interviewer per interview per row. It also calculates the interview step (see column description) and whether or not the interview is a many-to-one interview.
{% enddocs %}

{% docs interviewing_data_interviewers_and_scores %}
This base model is based on pre-aggregated data exported from Lever.
{% enddocs %}

{% docs interviewing_data_performance_of_hire %}
This data set indicates changes in compensation on a number of a different factors. 
{% enddocs %}

{% docs interview_data_hired_candidates_with_performance %}
This model joins the candidates current status with his or her accolades, if any. 

Future iterations of this analysis can take advantage of the way the jinja here has been written by updating bonus types in the lists set in the top. This will auto-generate new CTEs, columns, and joins appropriately.
{% enddocs %}

{% docs interviewing_data_interviewers_of_target_candidate %}
This model isolates interviewers who have interviewed candidates who have received a raise or a bonus or have been been terminated.
{% enddocs %}

{% docs experience_factor_col %}
"New Experience Factor post-promotion"
{% enddocs %}

{% docs candidate_score_col %}
"Candidate's Average Score based on all their interviewers"
{% enddocs %}

{% docs termination_col %}
"If candidates are terminated, it is voluntary or involuntary."
{% enddocs %}

{% docs status_col %}
"Candidates are Active, Probationary Period, or Terminated."
{% enddocs %}

{% docs candidate_id_col %}
"An hashed version of the candidates name is used to generate a candidate ID."
{% enddocs %}

{% docs candidate_name_col %}
"This value is not surfaced in Looker."
{% enddocs %}

{% docs interviewer_names_col %}
"Who interviewed the candidates?"
{% enddocs %}

{% docs interviewer_scores_col %}
"What score did the interviewer give? This is 1-4 with 1 being 'Strong No Hire', 2 being 'No Hire', 3 being 'Hire', and 4 being 'Strong Hire.'"
{% enddocs %}

{% docs is_many_to_one_interview_col %}
"Were there many people conducting the interview at the same time?"
{% enddocs %}