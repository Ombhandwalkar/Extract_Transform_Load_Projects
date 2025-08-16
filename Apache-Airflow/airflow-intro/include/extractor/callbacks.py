def _handle_failed_dag_run(context):
    print(f"""DAG run failed with the task {context["task_instance"].task_id} for the data interval between
          {context["prev_ds"]} and {context["next_ds"]}
    """)
    
def _handle_empty_size(context):
    print(f"There is no cocktail to process for the data interval between {context["prev_ds"]} and {context["next_ds"]}")
