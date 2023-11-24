from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task, task_group
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.context import Context
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
from kubernetes.client import models as k8s

PVC_NAME = 'pvc-pic' # CHANGE ME
IMAGE_NAME = 'gabinsc/sputnipic:latest' # CHANGE ME

MOUNT_PATH = '/data'
VOLUME_KEY  = 'volume-pic'
namespace = conf.get('kubernetes_executor', 'NAMESPACE')

params = {
    'inputlist': ['GEM_2D', 'GEM_3D', 'GEM_3D_small'],
}

@task
def list_inputs(params=None):
    return [ [f'out/{sim_name}/sim.inp'] for sim_name in params['inputlist'] ]


@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     params=params,

     # allow for passing lists in templated fields
     render_template_as_native_obj=True)
def pic():
    import os.path

    volume = k8s.V1Volume(
        name=VOLUME_KEY,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )
    volume_mount = k8s.V1VolumeMount(mount_path=MOUNT_PATH, name=VOLUME_KEY)

    # define a generic container, which can be used for all tasks
    container = k8s.V1Container(
        name='pic-container',
        image=IMAGE_NAME,
        working_dir=MOUNT_PATH,

        volume_mounts=[volume_mount]
    )
    pod_spec      = k8s.V1PodSpec(containers=[container], volumes=[volume])
    full_pod_spec = k8s.V1Pod(metadata=k8s.V1ObjectMeta(), spec=pod_spec)

    # 1 - Prepare input
    prepare_inputs = KubernetesPodOperator(
        task_id='prepare_inputs',
        full_pod_spec=full_pod_spec,

        cmds=['/pic/scripts/prepare_inputs.sh'],
        arguments="{{ params['inputlist'] }}",
    )

    # 2a - Launch PIC simulations
    picexec = KubernetesPodOperator.partial(
        task_id='pic-worker',
        full_pod_spec=full_pod_spec,

        cmds = ['/pic/sputniPIC'],
    ).expand(arguments=list_inputs())

    # 2b - Track the progress of all simulations
    @task.sensor(poke_interval=5, mode='reschedule')
    def tracker(dag_run = None):

        for ti in dag_run.get_task_instances():
            # if the task is ready to execute, it means that all workers are done
            if ti.task_id == 'clean' and ti.state is not None:
                return PokeReturnValue(is_done=True)

        return PokeReturnValue(is_done=False)

    track = tracker()

    # 3 - end of the workflow
    clean = KubernetesPodOperator(
        task_id='clean',
        full_pod_spec=full_pod_spec,

        trigger_rule = 'all_done',
        cmds = ['/pic/scripts/end_exec.sh'],
    )

    prepare_inputs >> picexec >> clean
    prepare_inputs >> track

pic()
