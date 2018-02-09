### Maturity

#### Alpha
 * Open source project
   * Dedicated non-gitlab.com TLD to decouple DNS and other things
   * Self-signed TLS certs, or self-purchased
   * Project folder in official gitlab GCP account (desired by Google)
   * Non-automated infrastructure (manual patching, etc.)
   * Open source repo, contributions
   * Advice from security leader
   * Advice from infrastructure team
   * No constraints on tearing down infrastructure or losing data
   * No requirements for central monitoring of infrastructure
   * Cloud-specific tuning (ie leveraging GCP Cloud SQL/BigQuery)
 * Internal usage
   * Data warehouse and configurations backed up
   * OAuth2 Proxy used for authentication with Google accounts

#### Beta

* tbd.gitlab.com subdomain
* Gitlab infra supplied SSL certificates
* Monitoring of critical systems by GitLab Infrastructure team
* Infrastructure automation: Docker, k8s helm
* Champions in targeted customer organization
* Data backups
* No marketing
* Cloud-agnostic

#### GA

* Sales can sell unlimited units without checking with PM or Engineering
* No data loss
* All components monitored by Infra team
* Product marketing: pricing packaging
