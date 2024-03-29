# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kfp.dsl import pipeline
from kfp import compiler
from kfp.dsl import component
from google.cloud import aiplatform

# TODO: Change with your project id and gcs bucket name
project_id = "YOUR-PROJECT-ID"
gcs_bucket = "YOUR-PROJECT-ID-vertex-ai-pipeline"
region = "us-central1"
pipeline_name = "practice-vertex-ai-pipeline"
pipeline_root_path = f"gs://{gcs_bucket}/{pipeline_name}"


@component
def step_one(text: str) -> str:
    print(text)
    return text


@component(base_image="python:3.9")
def step_two(text: str) -> str:
    print(text)
    return text


@component(base_image="python:3.9")
def step_three(text: str) -> str:
    print(text)
    return text


@component(packages_to_install=["google-cloud-storage"])
def step_four(text1: str, text2: str, gcs_bucket: str):
    from google.cloud import storage

    output_string = f"text1: {text1}; text2: {text2};"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob("practice-vertex-ai-pipeline/artefact/output.txt")
    blob.upload_from_string(output_string)

    print(output_string)


@pipeline(
    name="practice-vertex-ai-pipeline",
    description="Example of Vertex AI Pipeline",
    pipeline_root=pipeline_root_path,
)
def pipeline(text: str = "Hello"):
    step_one_task = step_one(text=text)
    step_two_task = step_two(text=step_one_task.output)
    step_three_task = step_three(text=step_one_task.output)
    step_four_task = step_four(
        text1=step_two_task.output,
        text2=step_three_task.output,
        gcs_bucket=gcs_bucket
    )


compiler.Compiler().compile(
    pipeline_func=pipeline, package_path=f"{pipeline_name}.json"
)

aiplatform.init(project=project_id, location=region)

job = aiplatform.PipelineJob(
    display_name="dp-practice-vertex-ai-pipeline",
    template_path=f"{pipeline_name}.json",
    pipeline_root=pipeline_root_path
)

job.submit()
