import streamlit as st
from datetime import datetime
import base64
import pandas as pd
import numpy as np
import string 
import re
import time
import gspread
import json
from google.oauth2.service_account import Credentials

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="SPRINGBOARD CAPITAL",
    page_icon="📊",
    layout="wide",
   initial_sidebar_state="expanded")
# ---------------- CUSTOM CSS ----------------
st.markdown("""
<style>
/* Hide default Streamlit header */
header {visibility: hidden;}

/* App background */
.stApp {    background-color: #eeeee4; }

/* Sidebar styling */
section[data-testid="stSidebar"] {
    background-color: #eb8f49;    color: black;  width: 220px;  min-width: 220px;  max-width: 220px;
}

/* Sidebar text color */
section[data-testid="stSidebar"] div, section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] p {  color: black;}

/* Hide the sidebar collapse/expand toggle in the global header */
div[data-testid="stSidebarCollapseButton"] {
    display: none;
}

/* Sticky custom top header */
.custom-header {   position: fixed;
    top: 0;  left: 205px;    width: 85%;
    background-color: #a67c5d;
    padding: 10px 20px;    display: flex;   align-items: center;  justify-content: space-between;  z-index: 1000;}

/* Logo style */
.custom-header img {  height: 60px;    margin-right: 70px;}

/* Header title */
.custom-header h1 {  color: white;  margin: 0; font-size: 45px; }

/* Refresh date */
.refresh-date {  color: white; font-size: 14px;}

/* Push page content below header */
body, .stApp > .main {
    padding-top: 70px;  /* height of header + some spacing */
}

</style>
""", unsafe_allow_html=True)



# ---------------- LOCAL LOGO ----------------
logo_path = "SBC_Logo.png"  # your local logo file

# Convert local image to base64 for HTML <img>
with open(logo_path, "rb") as f:
    logo_bytes = f.read()
logo_base64 = base64.b64encode(logo_bytes).decode()

# ---------------- CUSTOM HEADER ----------------
current_time = datetime.now().strftime("%Y-%m-%d")

st.markdown(f"""
<div class="custom-header">
    <div style="display:flex; align-items:center;">
        <img src="data:image/png;base64,{logo_base64}" alt="Logo">
        <h1>SPRINGBOARD CAPITAL</h1>
    </div>
    <div class="refresh-date">Last Refresh: {current_time}</div>
</div>
""", unsafe_allow_html=True)


# ---------------- SIDEBAR ----------------
st.sidebar.title("Reports Menu")

# Initialize session state
if "page" not in st.session_state:
    st.session_state.page = "overview"  # default page

# Helper function to render styled buttons
def sidebar_button(label, page_key):
    if st.sidebar.button(label, use_container_width=True):
        st.session_state.page = page_key
# def sidebar_button(label, page_key):
#     # Check if this button corresponds to the active page
#     is_active = st.session_state.page == page_key
#     # Render the button
#     if st.sidebar.button(label):
#         st.session_state.page = page_key

# Sidebar buttons with persistent highlight
sidebar_button("📊 Overview", "overview")
sidebar_button("📈 Arrears Tracker", "arrears")
sidebar_button("📋 Collections Tracker", "collections")


#------------- Reading Data--------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
credentials_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS_JSON"])
creds = Credentials.from_service_account_info( credentials_dict, scopes=SCOPES)

#helper funstions
def clean_columns(columns):
    seen = {};   new_cols = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            new_cols.append(col)
    new_cols =  [ re.sub(r"[^\w\s]", "", col).strip() for col in new_cols]
    
    return new_cols

def par_color(val):
    if val < 0.18: return "background-color: #b6f2c2"  # green
    elif 0.18 <= val <= 0.35: return "background-color: #ffe08a"  # amber
    else:  return "background-color: #ff9999"  # red

@st.cache_data(ttl=1800) ## loan register data
def load_loan_register():
    client = gspread.authorize(creds) # authorize credentials
    sheet_key = "1DEKCaV3PaXcnAbK8ZoQa4ty7CzArmCG2zMsDrETVzYE"
    # open google sheet
    spreadsheet = client.open_by_key(sheet_key)
    worksheet_id = 1503994147
    sheet = spreadsheet.get_worksheet_by_id(worksheet_id)
    df = pd.DataFrame(sheet.get_all_records()) # fetch all records of the specific sheet id

    df['Branch Code'] = df['Branch Code'].replace("KRK","RNG" )
    df.loc[df['ROName Loans']=='JOHN NJIRI KURIA','Branch Code'] = 'RECOVERY'
    #creating Category column
    bins = [0, 1, 31, 61, 91, float("inf")]
    labels = ["Performing", "1-30", "31-60", "61-90","91&Above"]
    df["Category"] = pd.cut( df["Days in Arrears"], bins=bins, labels=labels, include_lowest=True ,right=False)
    return df

@st.cache_data(ttl=600)  ## collections data
def load_collections_data(_creds):
    client = gspread.authorize(_creds)# authorize credentials
    sheet_key = "1JIuHZ5VM4veoitH8yF8KnZ38qKn-QTbPTqUJmO5b6t8"
    # open google sheet
    spreadsheet = client.open_by_key(sheet_key)
    worksheet_id = 1428160709
    sheet = spreadsheet.get_worksheet_by_id(worksheet_id)  # fetch all records of the specific sheet id
    coll_data = pd.DataFrame(sheet.get_all_values())

    # formarting data
    coll_data = coll_data.iloc[:, :31]
    coll_data.columns = clean_columns (coll_data.iloc[0, 0:31] )
    coll_data = coll_data.iloc[1:, 0:31]
    coll_data = coll_data.reset_index(drop=True)
    coll_data["Timestamp"] = pd.to_datetime(coll_data["Timestamp"],format="%m/%d/%Y %H:%M:%S")
    coll_data = coll_data[coll_data["Timestamp"] > "2025-01-01"]
    
    # cleaning and removing unwanted columns
    cols2 = ['Timestamp', 'Staff Name', 'File Number','Loan Type', 'Officers Comments', 'Outcomes','Demand Letter Type', 'Delivery Date']
    spl_loans = coll_data.loc[coll_data['Loan Type']=="SpringHela", cols2].rename(columns={'Officers Comments':'Officer Comments'} )
    cols1 = ['Timestamp', 'Staff Name', 'File Number','Loan Type','Officer Comments', 'Action','Demand Letter Type', 'Delivery Date']
    main_loans = coll_data.loc[coll_data['Loan Type']=="Main Loan", cols1].rename(columns={'Action':'Outcomes'} )
    coll_data = pd.concat([spl_loans, main_loans]).reset_index(drop=True).sort_values(by='Timestamp',ascending=False)
    
    return coll_data

with st.spinner("Loading Data...."):
    data = load_loan_register()

# global variables
cols = ['Branch Code', 'Member No', 'Loan No', 'Member Name', 'Loan Type',
            'Total Balance','Total In Arrears Loans', 'Days in Arrears', 'ROName Loans',"Category"]
df = data.loc[data['Outstanding Principle Balance']>1,cols]

#---------------- PAGE CONTENT ----------------

# Define page rendering functions
def render_overview():
    st.markdown( """ <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}
            </style> """, unsafe_allow_html=True )
    col1, col2 = st.columns([5,1], vertical_alignment="center")
    with col1: st.header("Portfolio Report Summary")
    with col2:
        if st.button("🔄 Loan Register", type="primary"):
            load_loan_register.clear()
    if data.empty:
        st.warning("No data found.")
        return

    # dff = df.set_index("Member No")
    # --- KPIs ---
    total_portfolio = df['Total Balance'].sum()
    total_arrears = df['Total In Arrears Loans'].sum()
    non_performing = df.loc[df['Days in Arrears']>0, 'Total Balance'].sum()
    par =  non_performing/ total_portfolio

    col1, col2, col3 = st.columns(3)

    # col1.metric("Total Portfolio", f"{total_portfolio:,.0f}")
    # col2.metric("Total Arrears", f"{total_arrears:,.0f}")
    # col3.metric("PAR", f"{par:,.2%}")
    with col1:
        st.markdown("#### 💼 Portfolio")
        st.markdown(f"#### {total_portfolio:,.0f}")
    with col2:
        st.markdown("#### ⚠️ Arrears")
        st.markdown(f"#### {total_arrears:,.0f}")
        st.caption(f"Arrear {total_arrears/total_portfolio:.1%} of loan book")
    with col3:
        st.markdown("#### 📉 PAR")
        st.markdown(f"#### {par:.2%}")
        st.caption(f"Non-Performing loan book: {non_performing/1_000_000:,.2f}M ")

    # --- Branch Statistics ---
    st.markdown("#### Branch Summary")
    with st.spinner("Loading Branch Summary...."):
        time.sleep(1)
        branch_stats = df.groupby("Branch Code").apply(
            lambda g: pd.Series({
                "Customers": g['Member No'].nunique(),
                "Portfolio": g['Total Balance'].sum(),
                "Arrears Amount": g['Total In Arrears Loans'].sum(),
                "PAR": (g.loc[g['Days in Arrears'] > 0, 'Total Balance'].sum() / g['Total Balance'].sum())
            })   ).reset_index()
        
        # Sort like a pivot table (risk view)
        branch_stats = branch_stats.sort_values("Portfolio", ascending=False)
        
        # Styling Table (Excel-like)        
        styled = (
            branch_stats.style
            # --- number formatting ---
            .format({ "Customers": "{:,}", "Portfolio": "{:,.0f}", "Arrears Amount": "{:,.0f}", "PAR": "{:.2%}" })
            # --- PAR conditional coloring ---
            .map(par_color, subset=["PAR"])  ) 
        # Display in Streamlit
        st.dataframe( styled.hide(axis="index"), use_container_width=True,  height=280 )

        ## Branch Ageing OLB
        st.markdown("#### Ageing Summary")
        category_arrears = pd.pivot_table(df,columns ='Category' ,index ='Branch Code' , values ='Total Balance',aggfunc='sum',
                   fill_value=0,margins=True,margins_name="Total")
        category_arrears["PAR"] = category_arrears.apply(lambda x: (x['Total']-x['Performing'])/x['Total'] , axis=1 )
        category_arrears.drop(columns="Performing",inplace=True )       
        # Styling Table (Excel-like)        
        styled = (
            category_arrears.style
            # --- number formatting ---
            .format({ "1-30": "{:,.0f}", "31-60": "{:,.0f}", "61-90": "{:,.0f}","91&Above": "{:,.0f}","Total": "{:,.0f}", "PAR": "{:.2%}" })
            # --- PAR conditional coloring ---
            .map(par_color, subset=["PAR"])   ) 
        # Display in Streamlit
        st.dataframe( styled.hide(axis="index"), use_container_width=True,  height=310 )

        st.markdown("#### PAR Summary")
        branch_par = pd.pivot_table(df, index='Branch Code', columns = 'Category', values = 'Total Balance',aggfunc='sum', margins=True,margins_name='Total')\
            .drop(columns='Performing' )\
            .apply(lambda x: x / x['Total']  ,axis=1)
        
        cols_to_sum = branch_par.columns.difference(["Total"])
        branch_par["Total"] = branch_par[cols_to_sum].sum(axis=1)
        styled1 = (
            branch_par.style
            # --- number formatting ---
            .format({ "1-30": "{:.2%}", "31-60": "{:.2%}", "61-90": "{:.2%}","91&Above": "{:.2%}","Total": "{:.2%}" })
            # --- PAR conditional coloring ---
            .map(par_color, subset=["Total"]) )
        # Display in Streamlit
        st.dataframe( styled1.hide(axis="index"), use_container_width=True,  height=310 )

    st.markdown("#### Portfolio Data")
    # ----- Preview LS ----
    with st.spinner("Loading Loan Register Preview...."):
        time.sleep(1)
        with st.expander("Preview Loan Register",icon="📋"):
            st.dataframe(df)


def render_arrears():
    st.header("Arrears Tracker")
    with st.spinner("Loading Arrears Summary...."):
        time.sleep(1)   
        st.markdown("#### RO Summary")
        ro_par = pd.pivot_table(df, index='ROName Loans', columns = 'Category', values = 'Total Balance',aggfunc='sum', margins=True,margins_name='Total')
        ro_par["PAR"] = ro_par.apply(lambda x: (x['Total']-x['Performing'])/x['Total'] , axis=1 )
        ro_par = ro_par.drop(columns="Performing")[["Total",'PAR',"1-30", "31-60","61-90","91&Above"]]
        styled2 = (
            ro_par.style
            # --- number formatting ---
            .format({ "PAR": "{:.2%}", "1-30": "{:,.0f}", "31-60": "{:,.0f}", "61-90": "{:,.0f}","91&Above": "{:,.0f}","Total": "{:,.0f}" })
            # --- PAR conditional coloring ---
            .map(par_color, subset=["PAR"]) )
        # Display in Streamlit
        st.dataframe( styled2.hide(axis="index"), use_container_width=True,  height=600 )

    with st.spinner("Loading Arrears Data...."):
        time.sleep(1)   
        dff = df.loc[df['Days in Arrears']>0,cols] # only to display loans in arrears
        with st.expander("Preview Data",icon="📋"):
            col1, col2, col3 = st.columns(3)
            with col1:
                ro_name = st.selectbox( "RO Name",options=["All"] + list(df['ROName Loans'].dropna().unique()) )
                if ro_name=="All": ro_name =df['ROName Loans'].unique() 
                else: ro_name= [ro_name]
            with col2:
                category_filter = st.selectbox( "Category",options=["All"] + list(df["Category"].dropna().unique()) )
                if category_filter=="All": category_filter =df["Category"].unique() 
                else: category_filter= [category_filter]
            with col3:
                branch_filter = st.selectbox( "Branch Code", options=["All"] + list(df["Branch Code"].dropna().unique()) )
                if  branch_filter=="All": branch_filter =df["Branch Code"].unique() 
                else: branch_filter= [branch_filter]
            
            # --- Apply filters dff
            filtered_df = dff[ (dff['ROName Loans'].isin(ro_name) )&
                    (dff["Category"].isin(category_filter)) &
                    (dff["Branch Code"].isin(branch_filter)) ].sort_values(by =['Days in Arrears','Member Name'],ascending=True)
            st.dataframe(filtered_df.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}" }) )

def render_collections():
    st.header("Collections and Demands")
    # initialize toggle state and session collections data
    if "coll_data" not in st.session_state:
        st.session_state.coll_data = pd.DataFrame()

    st.markdown( """ <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}
            </style> """, unsafe_allow_html=True )
    if st.button("Fetch Collection Remarks", type="primary"):
        with st.spinner("Loading Collections Data...."):
            time.sleep(1)
            # Load data into session_state, not local var
            st.session_state.coll_data = load_collections_data(creds)
    # 4. Access the persistent data from session_state
    coll_data = st.session_state.coll_data    

    # customer details on loans summary table
    cust_cols = ['Member Name', 'Loan No','Loan Type', 'Total Balance', 'Total In Arrears Loans', 'Days in Arrears']   
    search_val = st.text_input("Search File No", key="file_search")
    if search_val: 
        search_val = str( search_val.rjust(4,"0"))
        df['Member No'] = df['Member No'].apply(lambda x: str(str(x).rjust(4,"0")) ) 
        if search_val not in df['Member No'].unique():
            st.markdown(   """
                <div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White; ">
                    No such Active customer found </div>   """,    unsafe_allow_html=True )
            return
        else: 
            customer_details = df.loc[ (df['Member No'] == search_val ),cust_cols ]
            styled_table = ( customer_details.style.format({ "Total Balance": "{:,.0f}", "Total In Arrears Loans": "{:,.0f}" }) )
            st.dataframe(styled_table)
    else:  st.write("Enter file No to preview remarks")

            
    # --- Filter Collections Data -----
    if coll_data.empty:
        st.markdown(
                """
                <div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White; font-weight: bold;">
                    No data found ....Click Fetch remarks
                </div>
                """,    unsafe_allow_html=True )
        # st.warning("No data found....click toogle to fetch remarks")
        return
    with st.expander("Preview Customer Print Out",icon="📋"):
        if search_val:
            filtered_df = coll_data[ (coll_data['File Number'] == search_val) ]
            if filtered_df.empty: 
                st.warning("No remarks found")
            else:   st.dataframe(filtered_df)
        else: 
            st.write("Enter file No to preview remarks")
        
    st.markdown("#### RO Collections Summary")
    # getting RO Arrears + collections comments data
    arrears_data = df.loc[df['Days in Arrears']>0,:].groupby("Member No").\
                agg({ 'Branch Code':'max', 'Member Name':'max' , 'Total Balance':"sum", 'Total In Arrears Loans': "sum",
                      'Days in Arrears': "max", 'ROName Loans': 'max'}).reset_index()
    arrears_data['Member No'] = pd.to_numeric(arrears_data['Member No'], errors='coerce')
        
    remarks = coll_data.drop_duplicates(subset='File Number', keep = 'first')\
                   .loc[:,['File Number',"Timestamp",'Outcomes','Officer Comments']].copy()
    remarks['File Number'] = pd.to_numeric(remarks['File Number'], errors='coerce')
        
    customer_data = arrears_data.merge( remarks, how="left",left_on = 'Member No', right_on = 'File Number' , )\
                                    .drop(columns='File Number').fillna(" ").fillna(" ")
    customer_data['Timestamp'] =  pd.to_datetime(customer_data['Timestamp'], errors='coerce')\
                                    .dt.strftime('%d-%B-%Y').fillna(" ")
    with st.expander("Preview Summary",icon="📋"):
        col1, col2 = st.columns(2)
        with col1:
            ro_name = st.selectbox( "RO Name",options=["All"] + sorted(list(customer_data['ROName Loans'].dropna().unique()) ) )
            if ro_name=="All": ro_name = customer_data['ROName Loans'].unique() 
            else: ro_name= [ro_name]
        with col2:
            branch_filter = st.selectbox( "Branch Code", options=["All"] + list(customer_data["Branch Code"].dropna().unique()) )
            if  branch_filter=="All": branch_filter = customer_data["Branch Code"].unique() 
            else: branch_filter= [branch_filter]
        
        cols = ['Member Name', 'Total Balance', 'Total In Arrears Loans', 'Days in Arrears', 'Timestamp', 'Outcomes', 'Officer Comments']
        filtered_data = customer_data.loc [ (customer_data['ROName Loans'].isin(ro_name)) &
                                            (customer_data['Branch Code'].isin(branch_filter)),cols]\
                                    .sort_values(by='Days in Arrears',ascending=True)
        st.dataframe(filtered_data.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}" }))

## ..........End of Page Functions.........

def filter_list():
    category_filter = st.multiselect("Category", df["Category"].dropna().unique())
    branch_filter = st.multiselect("Branch Code", df["Branch Code"].dropna().unique())
    loan_type_filter = st.multiselect("Loan Type", df["Loan Type"].dropna().unique())
    ro_filter = st.multiselect("ROName Loans", df["ROName Loans"].dropna().unique())
#------------- RUNNING PAGES --------------
# Map page keys to functions
pages = {
    "overview": render_overview,
    "arrears": render_arrears,
    "collections": render_collections
}
       
# Render selected page
page_key = st.session_state.get("page")
if page_key in pages:
    pages[page_key]()  # Call the function for that page


