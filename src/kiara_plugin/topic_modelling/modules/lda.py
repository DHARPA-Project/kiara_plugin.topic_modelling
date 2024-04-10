# -*- coding: utf-8 -*-
from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException

class CreateDictionary(KiaraModule):
    """
    This module creates a dictionary, a mapping between words and their integer ids, from an array of tokens.
    This preliminary step is necessary to proceed with topic modelling techniques, such as Latent Dirichlet Allocation.
    The module enables optional configuration of the "filter_extremes" parameters, which can be used to remove tokens

    Dependencies:
    - Gensim: https://radimrehurek.com/gensim/corpora/dictionary.html
    """

    _module_type_name = "topic_modelling.dictionary_from_array"

    def create_inputs_schema(self):
        return {
            "corpus_tokens": {
                "type": "array",
                "doc": "Array that contains the tokens to process.",
            },
            "display_n_most_frequent": {
                "type": "integer",
                "doc": "Optionally return the most frequent tokens by specifying how many of them to display.",
                "optional": True,
                "default": False
            },
            "filter_extremes_no_below": {
                "type": "integer",
                "doc": "Keep tokens which are contained in at least no_below documents.",
                "optional": True,
                "default": False
            },
            "filter_extremes_no_above": {
                "type": "float",
                "doc": "Keep tokens which are contained in no more than no_above documents (fraction of total corpus size, not an absolute number).",
                "optional": True,
                "default": False
            },
            "filter_extremes_keep_n": {
                "type": "integer",
                "doc": "Keep only the first keep_n most frequent tokens.",
                "optional": True,
                "default": False
            },
            "filter_extremes_keep_tokens": {
                "type": "list",
                "doc": "List of tokens to keep.",
                "optional": True,
                "default": False
            }
        }

    def create_outputs_schema(self):
        return {
            "keys": {
                "type": "list",
                "doc": "The list of keys."
            },
            "words": {
                "type": "list",
                "doc": "The list of tokens."
            },
            "cfs": {
                "type": "list",
                "doc": "The list of collection frequencies, i.e., how many instances of a given token in the documents."
            },
            "dfs": {
                "type": "list",
                "doc": "How many documents contain a given token."
            },
            "most_common": {
                "type": "list",
                "doc": "If the option is enabled, returns most frequent tokens."
            }
        }

    def process(self, inputs, outputs):

        import gensim  # type: ignore
        from gensim.corpora import Dictionary  # type: ignore

        corpus_tokens = inputs.get_value_data("corpus_tokens")
        corpus_tokens_pa = corpus_tokens.arrow_array
        corpus_list = corpus_tokens_pa.to_pylist()

        no_below = inputs.get_value_data("filter_extremes_no_below") or False
        no_above = inputs.get_value_data("filter_extremes_no_above") or False
        keep_n = inputs.get_value_data("filter_extremes_keep_n") or False
        keep_tokens = inputs.get_value_data("filter_extremes_keep_tokens") or False

        dct = Dictionary(corpus_list)

        dct.filter_extremes(no_below=no_below, no_above)


        outputs.set_value_data("tokens_array", inputs.get_value_data("corpus_array"))
       