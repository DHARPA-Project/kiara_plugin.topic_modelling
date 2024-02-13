# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException

class GetLCCNMetadata(KiaraModule):
    """
    This module will get metadata from strings that comply with LCCN pattern: '/sn86069873/1900-01-05/' to get the publication references and the dates and add those informations as two new columns.
    In addition, if a mapping scheme is provided between publication references and publication names, it will add a column with the publication names.
    Such a map is provided in the form of a list of lists with publication references and publication names in the same order.
    Here is an example of how it should look:
    [["2012271201","sn85054967","sn93053873"],["Cronaca_Sovversiva","Il_Patriota","L'Indipendente"]]

    """

    _module_type_name = "topic_modelling.get_lccn_metadata"

    def create_inputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "The corpus for which we want to get metadata from file names.",
            },
            "file_name_col": {
                "type": "string",
                "doc": "The column containing file names with metadata. In order to work, file names need to comply with LCCN pattern '/sn86069873/1900-01-05/' containing publication reference and date.",
            },
            "map": {
                "type": "list",
                "doc": "List of lists of unique publications references and publication names in the collection provided in the same order.",
                "optional": True,
            },
        }

    def create_outputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "Augmented table containing extracted metadata.",
            }
        }

    def process(self, inputs, outputs) -> None:
        import re
        import polars as pl


        table_obj = inputs.get_value_obj("corpus_table")
        column_name = inputs.get_value_obj("file_name_col").data
        pub_refs, pub_names = None, None

        try:
            pub_refs = inputs.get_value_obj("map").data[0]
            pub_names = inputs.get_value_obj("map").data[1]

        except Exception:
            pass

        sources = pl.from_arrow(table_obj.data.arrow_table)

        def get_ref(file):
            try:
                ref_match = re.findall(r"(\w+\d+)_\d{4}-\d{2}-\d{2}_", file)
                if not ref_match:
                    return None
                return ref_match[0]

            except Exception as e:
                raise KiaraProcessingException(e)

        def get_date(file):
            try:
                date_match = re.findall(r"_(\d{4}-\d{2}-\d{2})_", file)
                if not date_match:
                    return None
                return date_match[0]
            except Exception as e:
                msg = f"Error in get_date: {e}"
                raise KiaraProcessingException(msg)

        try:
            sources = sources.with_columns(
                [
                    sources[column_name].apply(get_date).alias("date"),
                    sources[column_name].apply(get_ref).alias("publication_ref"),
                ]
            )
        except Exception as e:
            msg = f"An error occurred while augmenting the dataframe: {e}"
            raise KiaraProcessingException(msg)

        try:
            if pub_refs and pub_names:
                pub_ref_to_name = dict(zip(pub_refs, pub_names))
                sources = sources.with_columns(
                    sources["publication_ref"]
                    .apply(lambda x: pub_ref_to_name.get(x, None))
                    .alias("publication_name")
                )
        except Exception:
            raise KiaraProcessingException(msg)

        try:
            output_table = sources.to_arrow()

        except Exception as e:
            raise KiaraProcessingException(e)

        outputs.set_value("corpus_table", output_table)


class CorpusDistTime(KiaraModule):
    """
    This module aggregates a table by day, month or year from a corpus table that contains a date column. It returns the distribution over time, which can be used for display purposes, such as visualization.

    Dependencies:
    - polars: https://www.pola.rs/
    - pyarrow: https://arrow.apache.org/docs/python/
    - duckdb: https://duckdb.org/

    """

    _module_type_name = "topic_modelling.time_dist"

    def create_inputs_schema(self):

        return {
            "periodicity": {
                "type": "string",
                "type_config": {"allowed_strings": ["day", "month", "year"]},
                "doc": "The desired data periodicity to aggregate the data. Values can be either 'day','month' or 'year'.",
            },
            "date_col_name": {
                "type": "string",
                "doc": "Column name of the column that contains the date. Values in this column need to comply with date format: https://docs.rs/chrono/latest/chrono/format/strftime/index.html.",
            },
            "title_col_name": {
                "type": "string",
                "doc": "Column name of the values containing publication names or ref/id. This column will be used in the output.",
            },
            "corpus_table": {
                "type": "table",
                "doc": "The corpus table for which the distribution over time is needed.",
            },
        }

    def create_outputs_schema(self):
        return {"dist_table": {"type": "table", "doc": "The aggregated data table."}}

    def process(self, inputs, outputs) -> None:
        import duckdb
        import polars as pl

        agg = inputs.get_value_obj("periodicity").data
        title_col = inputs.get_value_obj("title_col_name").data
        time_col = inputs.get_value_obj("date_col_name").data

        table_obj = inputs.get_value_obj("corpus_table")

        sources = pl.from_arrow(table_obj.data.arrow_table)

        sources = sources.with_columns(
            pl.col(time_col).str.strptime(pl.Date, "%Y-%m-%d")
        )

        if agg == "month":
            query = f"SELECT EXTRACT(MONTH FROM date) AS month, EXTRACT(YEAR FROM date) AS year, {title_col}, COUNT(*) as count FROM sources GROUP BY {title_col}, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)"  # noqa
        elif agg == "year":
            query = f"SELECT EXTRACT(YEAR FROM date) AS year, {title_col}, COUNT(*) as count FROM sources GROUP BY {title_col}, EXTRACT(YEAR FROM date)"  # noqa
        elif agg == "day":
            query = f"SELECT date, {title_col}, COUNT(*) as count FROM sources GROUP BY {title_col}, date"  # noqa

        queried_df = duckdb.query(query)

        pa_out_table = queried_df.arrow()

        outputs.set_value("dist_table", pa_out_table)
