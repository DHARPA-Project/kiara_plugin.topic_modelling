import streamlit as st
from kiara.api import KiaraAPI


kiara = KiaraAPI.instance()

st.header("1. Onboarding")

tab_ob_local, tab_ob_github, tab_ob_zenodo = st.tabs(["Local folder", "Github", "Zenodo archive"])

with tab_ob_local:
    st.subheader("Get files from local folder")
  
    ob_local_files_form = st.form(key="local_files_form")

    #### user inputs ####
    local_folder_path = ob_local_files_form.text_input(
        "Local folder path:",
        "/path/to/folder",
        key = "local-folder-path",
    )

    ob_local_files_comment = ob_local_files_form.text_input(
            "Comment:",
            "Description of files.",
            key="ob-local-files-comment",
        )

    ob_local_submitted = ob_local_files_form.form_submit_button("Confirm")

    ##### Run operation #####

    if ob_local_submitted == True:
        
        ob_local_file_bundle_gh_inputs = {
        "path": local_folder_path,
        }



        try:
            ob_local_file_bundle_results = kiara.run_job('import.table.from.local_folder_path', inputs=ob_local_file_bundle_gh_inputs, comment=ob_local_files_comment)
            ob_local_file_bundle_results_data = ob_local_file_bundle_results['table'].data
        
            #### Display results ####
            ob_local_files_res = st.form(key="ob_local_files_res")
            ob_local_files_col1, ob_local_files_col2 = st.columns(2)

            """
            with ob_local_files_col1:
                gh_files_results_confirmed = gh_files_res.form_submit_button("Confirm")
                
            with ob_local_files_col2:
                st.write(f"Number of files: {dl_file_bundle_gh_results_data.number_of_files}")
                st.json(dl_file_bundle_gh_results_data.included_files) 
            """       

        except Exception as e:
            st.error(f"An error occurred: {e}")



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
    
    
