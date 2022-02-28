import ramda as R
from airflow.models import DagBag


def test_sync_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_tasks_executes_without_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    dag = dag_bag.get_dag("shop_289_product_upload")
    R.pipe(R.map(lambda task_id: dag.get_task(task_id)), R.map(lambda task: task.execute({})))(dag.task_ids)
