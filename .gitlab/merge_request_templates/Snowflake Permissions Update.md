## Issue
<!---
Link the Issue this MR closes
--->
Closes #

## Checklist

### Format

- [ ] YAML validator passes

### Users

- [ ] If new user, confirm there is a corresponding user role with `securityadmin` as the owner

### Roles

- [ ] If new role, confirm it has been created in Snowflake with `securityadmin` as the owner
- [ ] Confirm user is only granted to user role - can be overridden if necessary

### Warehouse

- [ ] Confirm warehouse is created in Snowflake and matches size
