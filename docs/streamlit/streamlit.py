import streamlit as st
from kiara.api import KiaraAPI

kiara = KiaraAPI.instance()

if 'ob_local_submitted' not in st.session_state:
    st.session_state.ob_local_submitted = False

if 'ob_local_files_res_submitted' not in st.session_state:
    st.session_state.ob_local_files_res_submitted = False

def ob_local_submitted_clicked():
    st.session_state.ob_local_submitted = True

def ob_local_files_res_clicked():
    st.session_state.ob_local_files_res_submitted = True


with st.sidebar:
    st.header("Data lineage")
    st.write("Select an element of your data store to view its lineage.")
    store_items = []
    store_content = kiara.retrieve_values_info().dict()
    
    for item_id, item in store_content['item_infos'].items():
        store_item = {}
        store_item['value_created'] = item['value_created']
        store_item['type'] = item['value_schema']['type']
        store_item['value_size'] = item['value_size']
        store_item['aliases'] = item['aliases']
        store_items.append(store_item)

    sb_data_store_form = st.form(key="sb_data_store_form")
    aliases_list = [alias for item in store_items for alias in item.get("aliases", [])]
    
    alias_form = sb_data_store_form.multiselect(label="Select data", options=aliases_list)

    # if alias_form:



    
 


    # data store
    # data lineage
    # operations

st.header("Data Onboarding")

tab_ob_local, tab_ob_github, tab_ob_zenodo, tab_ob_store = st.tabs(["Local folder", "Github", "Zenodo archive", "Data store"])
# add data store as onboarding source


# tab get data from local folder
with tab_ob_local:
    st.subheader("Get files from local folder")
  
    ob_local_files_form = st.form(key="local_files_form")

    #### user inputs ####
    local_folder_path = ob_local_files_form.text_input(
        "Local folder path:",
        "/Users/mariella.decrouychan/Documents/GitHub/kiara_plugin.topic_modelling/tests/resources/data/text_corpus/data",
        key = "local-folder-path",
    )

    ob_local_files_comment = ob_local_files_form.text_input(
            "Comment:",
            "Description of files.",
            key="ob-local-files-comment",
        )

    ob_local_submitted = ob_local_files_form.form_submit_button("Confirm", on_click=ob_local_submitted_clicked)

    ##### Run operation #####

    if st.session_state.ob_local_submitted:
        
        ob_local_file_bundle_gh_inputs = {
        "path": local_folder_path,
        }

        try:
            ob_local_file_bundle_results = kiara.run_job('import.table.from.local_folder_path', inputs=ob_local_file_bundle_gh_inputs, comment=ob_local_files_comment)
            ob_local_file_bundle_results_data = ob_local_file_bundle_results['table'].data

        except Exception as e:
            st.error(f"An error occurred: {e}")
        
        #### Display results ####
        ob_local_files_res = st.form(key="ob_local_files_res")

        ob_local_files_res.write("Check the result in display preview below and name the table before confirming to add it to the data store.")

        ob_local_files_name = ob_local_files_res.text_input(
                "Name:",
                "my_data_item_name",
                key="ob-save-table-name",
            )
        
        ob_local_files_res_submitted = ob_local_files_res.form_submit_button("Save to data store", on_click=ob_local_files_res_clicked)

        if st.session_state.ob_local_files_res_submitted:
            try:
                kiara.store_value(ob_local_file_bundle_results['table'], ob_local_files_name)
                ob_local_files_res.write("Data item added to the data store.")
            except Exception as e:
                ob_local_files_res.error(f"An error occurred: {e}")

        with ob_local_files_res.expander("Display preview"):
            st.dataframe(ob_local_file_bundle_results_data.to_pandas_dataframe())


with tab_ob_github:
    st.subheader("Get files from Github repository")
    gh_files_form = st.form(key="github_files_form")

    #### user inputs ####
    repo_user = gh_files_form.text_input(
            "Repository user name:",
            "DHARPA-Project",
            key="rep-user",
        )
    repo_name = gh_files_form.text_input(
            "Repository name:",
            "kiara.examples",
            key="rep-name",
        )
    sub_path = gh_files_form.text_input(
            "Sub-path:",
            "kiara.examples-main/examples/workshops/dh_benelux_2023/data",
            key="sub-path",
        )

    incl_files_type = gh_files_form.text_input(
            "Include files:",
            "txt",
            key="incl-files",
        )

    gh_files_comment = gh_files_form.text_input(
            "Comment:",
            "Import files from Github repository.",
            key="gh-files-comment",
        )

    gh_files_submitted = gh_files_form.form_submit_button("Confirm")

    ##### Run operation #####

    if gh_files_submitted == True:
        dl_file_bundle_gh_inputs = {
        "user": repo_user,
        "repo": repo_name,
        "sub_path": sub_path,
        "include_files": [incl_files_type],
        }
        
        try:
            dl_file_bundle_gh_results = kiara.run_job('download.file_bundle.from.github', inputs=dl_file_bundle_gh_inputs, comment=comment)
            dl_file_bundle_gh_results_data = dl_file_bundle_gh_results['file_bundle'].data
            
            #### Display results ####
            gh_files_res = st.form(key="github_files_res")
            gh_files_col1, gh_files_col2 = st.columns(2)

            
            with gh_files_col1:
                gh_files_results_confirmed = gh_files_res.form_submit_button("Confirm")
                
            with gh_files_col2:
                st.write(f"Number of files: {dl_file_bundle_gh_results_data.number_of_files}")
                st.json(dl_file_bundle_gh_results_data.included_files)        

        except Exception as e:
            st.error(f"An error occurred: {e}")
    
    
