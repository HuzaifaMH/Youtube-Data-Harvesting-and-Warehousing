import streamlit as st
from streamlit_option_menu import option_menu
import mongo, query, sql
st.set_page_config(
    page_icon="▶",
    page_title="Youtube Warehousing"
)
class MultiApp:
    def __init__(self):
        self.apps=[]
    def add_app(self, title, function):
        self.apps.append({"title":title,
                          "function":function})
    def run():
        with st.sidebar:
            selected=option_menu(menu_title="Menu",
            options=['Home','MongoDB','Migration to SQL',"Query"],
            icons=['house','database','filetype-sql','question-circle-fill'],
            menu_icon='list',
            #styles={},
            default_index=0
        )
        #Link for Bootstrap Icons: https://icons.getbootstrap.com/    
        if selected == "Home":
            st.write("Youtube Data Harvesting and Warehousing")
        elif selected == "MongoDB":
            mongo.app()
        elif selected == "Migration to SQL":
            sql.app()
        elif selected == "Query":
            query.app()
    run()