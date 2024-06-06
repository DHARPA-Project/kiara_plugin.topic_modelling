# -*- coding: utf-8 -*-
from kiara.api import KiaraModule

# These modules are temporary and will be removed in the future when features availables from Kiara onboarding modules.


class CreateTableFromZenodo(KiaraModule):
    """
    This module retrieves text files from a specified folder hosted on Zenodo.
    It takes the DOI and the name of the file as inputs.
    It outputs a table with two columns: one for the file names and the other for the content of these files.

    """

    _module_type_name = "topic_modelling.create_table_from_zenodo"

    def create_inputs_schema(self):
        return {
            "doi": {
                "type": "string",
                "doc": "The Digital Object Identifier for the resource."
            },
            "file_name": {
                "type": "string",
                "doc": "The name of the file to be processed."
            }
        }

    def create_outputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "A table with two columns: file names and their contents."
            }
        }

    def process(self, inputs, outputs):
        import io
        import os
        import zipfile

        import polars as pl
        import requests

        doi = inputs.get_value_data("doi")
        file_name = inputs.get_value_data("file_name")
        url = f"https://zenodo.org/record/{doi}/files/{file_name}"

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        zip_file_bytes = io.BytesIO(response.read())

        # Process text files and create the table
        file_names = []
        file_contents = []
        with zipfile.ZipFile(zip_file_bytes, "r") as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith(".txt"):
                    with zip_ref.open(file) as f:
                        content = f.read().decode("utf-8")  # Assuming text files are UTF-8 encoded
                        file_names.append(os.path.basename(file))
                        file_contents.append(content)

        # Create a table with polars
        pl_df = pl.DataFrame({"file_name": file_names, "content": file_contents})
        pa_table = pl_df.to_arrow()

        outputs.set_value("corpus_table", pa_table)