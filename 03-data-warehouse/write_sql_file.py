import os
from string import Template

# Read template
with open("big_query.sql.template", "r") as template_file:
    template = Template(template_file.read())

# Replace placeholders
query = template.substitute(
    project_id=os.getenv("PROJECT_ID"),
    dataset=os.getenv("DATASET"),
    gcs_bucket=os.getenv("GCS_BUCKET"),
)

# Save the compiled query
with open("big_query.sql", "w") as query_file:
    query_file.write(query)

print("Query compiled successfully!")