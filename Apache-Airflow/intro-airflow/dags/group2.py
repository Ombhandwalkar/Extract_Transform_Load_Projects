from airflow.sdk import dag, task, group, task_group 



@dag
def group():
    @task
    def a():
        return 42
    
    @task_group(default_args={'retires':2})
    def my_group(val:int):
        @task
        def b(my_val:int):
            print(my_val + 42)

        @task_group(default_args={'retries':3})
        def my_nested_group():
            @task
            def c():
                print('c')
            c()
        val=a()
        my_group(val)
    group()