import json
import os
from datetime import timedelta
from functools import partial
import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

openai_url = "/openai/invoke"
flow_mapping_internet_url = "/flow_mapping_internet/invoke"
flow_mapping_cas_url = "/flow_mapping_cas/invoke"

session_id = "20240131"


def post_request(
    post_url: str, formatter: callable, ti: any, data_to_send: dict = None, **kwargs
):
    if not data_to_send:
        data_to_send = formatter(ti, **kwargs)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=600) as client:
        try:
            response = client.post(post_url, json=data_to_send)

            response.raise_for_status()

            data = response.json()

            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")


def task_PyOpr(
    task_id: str,
    callable_func,
    # retries: int = 3,
    # retry_delay=timedelta(seconds=3),
    execution_timeout=timedelta(minutes=10),
    op_kwargs: dict = None,
):
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        # retries=retries,
        # retry_delay=retry_delay,
        execution_timeout=execution_timeout,
        op_kwargs=op_kwargs,
    )


def agent_formatter(
    ti, prompt: str = "", task_ids: list = None, session_id: str = "", **kwargs
):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            if "output" in data:
                task_output = data["output"].get(
                    "content", data["output"].get("output")
                )
            else:
                task_output = "No output"
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        formatted_data = {
            "input": {"input": prompt + "\n\n INPUT:<" + content + ">"},
            "config": {"configurable": {"session_id": session_id}},
        }
        return formatted_data
    else:
        pass


def init_openai_formatter(ti, prompt: str = None, **kwargs):
    query = kwargs["dag_run"].conf.get("query", "None")
    content = prompt + "\n\n<" + query + ">"
    formatted_data = {"input": content}
    return formatted_data


def openai_formatter(ti, prompt: str = None, task_ids: list = None):
    results = []
    for task_id in task_ids:
        data = ti.xcom_pull(task_ids=task_id)
        task_output = data["output"]["output"]  # Output from agent
        task_output_with_id = (
            f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
        )
        results.append(task_output_with_id)
    content = "\n\n".join(results)  # Join all task results into a single string
    if prompt:
        content = prompt + "\n\n" + content
    formatted_data = {"input": content}
    return formatted_data


flow_query_parse_formatter = partial(
    init_openai_formatter,
    prompt="""
        You are a world class algorithm for extracting information to output JSON.
        Extract information from the bracket below and organize it into JSON with keys of "name", "category", and "source".
        "name" must be the chemical substance name in english without any description.
        "Category" must be one of the follows:
            Emissions to fresh water
            Emissions to sea water
            Emissions to water, unspecified
            Emissions to water, unspecified (long-term)
            Emissions to agricultural soil
            Emissions to non-agricultural soil
            Emissions to soil, unspecified
            Emissions to soil, unspecified (long-term)
            Emissions to urban air close to ground
            Emissions to non-urban air or from high stacks
            Emissions to lower stratosphere and upper troposphere
            Emissions to air, unspecified
            Emissions to air, unspecified (long-term)
            Non-renewable material resources from ground
            Non-renewable element resources from ground
            Non-renewable energy resources from ground
            Renewable element resources from ground
            Renewable energy resources from ground
            Renewable material resources from ground
            Renewable resources from ground, unspecified
            Non-renewable resources from ground, unspecified
            Non-renewable material resources from water
            Non-renewable element resources from water
            Non-renewable energy resources from water
            Renewable element resources from water
            Renewable energy resources from water
            Renewable material resources from water
            Renewable resources from water, unspecified
            Non-renewable resources from water, unspecified
            Non-renewable material resources from air
            Non-renewable element resources from air
            Non-renewable energy resources from air
            Renewable element resources from air
            Renewable energy resources from air
            Renewable material resources from air
            Renewable resources from air, unspecified
            Non-renewable resources from air, unspecified
            Renewable element resources from biosphere
            Renewable energy resources from biosphere
            Renewable material resources from biosphere
            Renewable genetic resources from biosphere
            Renewable resources from biosphere, unspecified
        "source" means the natural source of the chemical substance. Leave it blank if not provided.
        """,
)

flow_synonyms_retrieve_formatter = partial(
    agent_formatter,
    prompt="List top 5 synonyms of the INPUT in the follwing bracket in English and output as json. The INPUT must be listed as one of the synonyms.",
    task_ids=["flow_query_parse"],
)

flow_cas_retrieve_formatter = partial(
    agent_formatter,
    prompt="Retrieve the CAS number for each synonym in the INPUT in the follwing bracket.",
    task_ids=["flow_synonyms_retrieve"],
)

flow_query_parse = partial(
    post_request, post_url=openai_url, formatter=flow_query_parse_formatter
)

flow_synonyms_retriever = partial(
    post_request,
    post_url=flow_mapping_internet_url,
    formatter=flow_synonyms_retrieve_formatter,
)

flow_cas_retriever = partial(
    post_request, post_url=flow_mapping_cas_url, formatter=flow_cas_retrieve_formatter
)

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="flow_mapping1",
    default_args=default_args,
    description="LCA flows mapping",
    schedule_interval=None,
    tags=["flow_mapping1"],
    catchup=False,
) as dag:
    flow_query_parse = task_PyOpr(
        task_id="flow_query_parse",
        callable_func=flow_query_parse,
    )

    flow_synonyms_retrieve = task_PyOpr(
        task_id="flow_synonyms_retrieve", callable_func=flow_synonyms_retriever
    )

    flow_cas_retrieve = task_PyOpr(
        task_id="flow_cas_retrieve", callable_func=flow_cas_retriever
    )

    flow_query_parse >> flow_synonyms_retrieve >> flow_cas_retrieve
