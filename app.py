import streamlit as st
from datetime import datetime
import base64
import pandas as pd
import numpy as np
import string 
import re
import gspread
import json
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from dateutil.relativedelta import relativedelta

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
# Ensure you have SBC_Logo.png in the same directory
try:
    logo_path = "SBC_Logo.png"
    with open(logo_path, "rb") as f:
        logo_bytes = f.read()
    logo_base64 = base64.b64encode(logo_bytes).decode()
    has_logo = True
except FileNotFoundError:
    has_logo = False
    logo_base64 = "" # Fallback if image missing

# ---------------- CUSTOM HEADER ----------------
logo_html = f'<img src="data:image/png;base64,{logo_base64}" alt="Logo">' if has_logo else ""
st.markdown(f"""
<div class="custom-header">
    <div style="display:flex; align-items:center;">
        {logo_html}
        <h1>SPRINGBOARD CAPITAL</h1>
    </div>
""", unsafe_allow_html=True)


# ---------------- AUTH & CREDENTIALS SETUP ----------------
READ_SCOPES = (  "https://www.googleapis.com/auth/spreadsheets.readonly",)
WRITE_SCOPES = ( "https://www.googleapis.com/auth/drive",)
try:
    credentials_dict = dict(st.secrets["GOOGLE_CREDENTIALS_JSON"])
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(credentials_dict, scopes=READ_SCOPES)
except Exception as e:
    st.error(f"Google credentials error: {e}")
    st.stop()
    
@st.cache_resource
def get_gspread_client(scopes_tuple):
    credentials_dict = dict(st.secrets["GOOGLE_CREDENTIALS_JSON"])
    credentials_dict["private_key"] = (credentials_dict["private_key"]   .replace("\\n", "\n")  )
    
    creds = Credentials.from_service_account_info(  credentials_dict,scopes=list(scopes_tuple)    )
    return gspread.authorize(creds)
    
# ---------------- HELPER FUNCTIONS ----------------
def clean_columns(columns):
    seen = {};   new_cols = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            new_cols.append(col)
    new_cols = [ re.sub(r"[^\w\s]", "", col).strip() for col in new_cols]
    return new_cols

def par_color(val):
    if val < 0.18: return "background-color: #b6f2c2"
    elif 0.18 <= val <= 0.35: return "background-color: #ffe08a"
    else:  return "background-color: #ff9999"

# ---------------- DATA LOADING & PROCESSING ----------------
@st.cache_data
def read_excel_file(file):
    return pd.read_excel(file, engine="openpyxl")

@st.cache_data(ttl=7200) 
def load_loan_register():
    client = get_gspread_client(READ_SCOPES)
    sheet_key = "1DEKCaV3PaXcnAbK8ZoQa4ty7CzArmCG2zMsDrETVzYE"
    spreadsheet = client.open_by_key(sheet_key)
    worksheet_id = 1503994147
    sheet = spreadsheet.get_worksheet_by_id(worksheet_id)
    df = pd.DataFrame(sheet.get_all_records())  

    df['Loan No'] = df['Loan No'].astype(str)
    df['Branch Code'] = df['Branch Code'].replace("KRK","RNG" )
    df.loc[df['ROName Loans']=='JOHN NJIRI KURIA','Branch Code'] = 'RECOVERY'
    
    bins = [0, 1, 31, 61, 91, float("inf")]
    labels = ["Performing", "1-30", "31-60", "61-90","91&Above"]
    df["Category"] = pd.cut(df["Days in Arrears"], bins=bins, labels=labels, include_lowest=True, right=False)

    cols_to_use = ['Branch Code', 'Member No', 'Loan No', 'Member Name', 'Loan Type',
                   'Total Balance','Total In Arrears Loans', 'Days in Arrears', 'ROName Loans','Category']
    if not all(col in df.columns for col in cols_to_use):
        raise ValueError("Column mismatch in loan register data") 
    # Return the filtered dataframe directly
    return df.loc[df['Outstanding Principle Balance'] > 1, cols_to_use]


@st.cache_data(ttl=7200) 
def load_disbursements(data):
    client = get_gspread_client(READ_SCOPES)
    sheet_key = "1if6aSMKJKAYkXIruzqCgMZ_E5ybC2ivc2a7KOspr5NQ"
    spreadsheet = client.open_by_key(sheet_key)
    worksheet_id = 385373648 
    values =  spreadsheet.get_worksheet_by_id(worksheet_id).get("A:F") 
    dis_tat = pd.DataFrame(values[2:], columns=values[1])

    cols_to_use = ['Date','File No', 'New Money']
    if not all(col in dis_tat.columns for col in cols_to_use):
        raise ValueError("Columns mismatch")  
    
    # filtering require month
    start_of_month = datetime.today().replace( day=1,hour=0, minute=0, second=0, microsecond=0)
    dis_tat = dis_tat.loc[~(dis_tat.Date.isna()),:]
    dis_tat["Date"] = pd.to_datetime(dis_tat["Date"],format="%d-%b-%y" ,errors="coerce")
    dis_tat = dis_tat[dis_tat["Date"] > start_of_month ]
    dis_tat = dis_tat.reset_index(drop=True)
    
    # getiing RO and Branch
    dis_tat['File No'] = dis_tat['File No'].astype(int)
    dis_tat['New Money'] = dis_tat['New Money'].apply(lambda x: x.replace(",","")).astype(float)
    
    mapped_list = data.groupby("Member No").agg({ 'Member Name':'max', 'Branch Code':'max' ,'ROName Loans': 'max'}).reset_index()
    
    dis_tat['ROName'] = dis_tat['File No'].map(mapped_list.set_index('Member No')['ROName Loans'])
    dis_tat['Branch'] = dis_tat['File No'].map(mapped_list.set_index('Member No')['Branch Code'])

    # merging SPL LOANS
    cols = ['Disbursement Date','Member Name','Member No','Loan No','Approved Amount','Branch Code', 'ROName Loans']
    data["Disbursement Date"] = pd.to_datetime(data["Disbursement Date"])
    dis_spl = data.loc[data['Disbursement Date']>=start_of_month,cols]
    dis_spl['New Money'] = dis_spl['Approved Amount']
    # renaming columns
    dis_spl = dis_spl.rename(columns={'Disbursement Date':'Date','Member Name': 'Customer Name','Member No': 'File No','Approved Amount':'Gross Amount' ,
                        'Branch Code': 'Branch','ROName Loans': 'ROName'})
    # Whole disbursements data
    dis_data = pd.concat([dis_tat, dis_spl], ignore_index=True)

    # Return the filtered dataframe directly
    return dis_tat

@st.cache_data(ttl=3600)
def load_collections_data(_creds):
    client = gspread.authorize(_creds)
    sheet_key = "1JIuHZ5VM4veoitH8yF8KnZ38qKn-QTbPTqUJmO5b6t8"
    spreadsheet = client.open_by_key(sheet_key)
    worksheet_id = 1428160709
    sheet = spreadsheet.get_worksheet_by_id(worksheet_id)
    coll_data = pd.DataFrame(sheet.get_all_values())

    coll_data = coll_data.iloc[:, :31]
    coll_data.columns = clean_columns(coll_data.iloc[0, 0:31])
    coll_data = coll_data.iloc[1:, 0:31]
    coll_data = coll_data.reset_index(drop=True)
    coll_data["Timestamp"] = pd.to_datetime(coll_data["Timestamp"],format="%m/%d/%Y %H:%M:%S")
    coll_data = coll_data[coll_data["Timestamp"] > "2025-01-01"]
    
    cols2 = ['Timestamp', 'Staff Name', 'File Number','Loan Type', 'Officers Comments', 'Outcomes','Demand Letter Type', 'Delivery Date']
    spl_loans = coll_data.loc[coll_data['Loan Type']=="SpringHela", cols2].rename(columns={'Officers Comments':'Officer Comments'} )
    cols1 = ['Timestamp', 'Staff Name', 'File Number','Loan Type','Officer Comments', 'Action','Demand Letter Type', 'Delivery Date']
    main_loans = coll_data.loc[coll_data['Loan Type']=="Main Loan", cols1].rename(columns={'Action':'Outcomes'} )
    coll_data = pd.concat([spl_loans, main_loans]).reset_index(drop=True).sort_values(by='Timestamp',ascending=False)
    return coll_data

def clean_loan_register(loan_register, customer_list):
    #upload ls to google sheets
    #index where column names start at
    index_ = loan_register[loan_register.iloc[:,0] =="Branch Code"].index[0]
    
    # getting the data from row 10 downwards
    loan_register = loan_register.iloc[10:,:].reset_index(drop=True)
    
    # setting column names
    loan_register.columns = loan_register.iloc[0]
    loan_register.drop(labels=0,axis=0, inplace = True)
    loan_register.reset_index(drop=True, inplace = True)
    
    # picking columns which are not blanks
    loan_register = loan_register.loc[:,loan_register.columns.notna()]
    
    # rows where customer_no and loan_no are not blank
    loan_register = loan_register[~(loan_register['Member No'].isna()) & ~(loan_register['Loan No'].isna() )]
    
    # Formating customer numbers
    loan_register["Member No"] = loan_register["Member No"].apply(lambda x: str(int(x)).rjust(10,"0"))
    loan_register["Member No"] = loan_register["Member No"].apply(lambda x: str(int(x)).rjust(10,"0"))
    customer_list["No."] = customer_list["No."].apply(lambda x: str(int(x)).rjust(10,"0"))
    customer_list.drop_duplicates(subset='No.',inplace=True)
    
    # fetching ro names and Formarting
    loan_register['ROName Loans'] = loan_register['Member No'].map(customer_list.set_index('No.')['Relationship Officer Name'])
    loan_register['ROName Loans']  = loan_register['ROName Loans'].apply( lambda x: x if pd.isnull(x) else x.upper().strip() )
    loan_register['ROName Loans'] = loan_register['ROName Loans'].replace({
                                                                    'JEREMIAH MUSEE': 'JEREMIAH WAMBUA',
                                                                    "MERCY NG'ANGA": "MERCY NJERI NG'ANG'A",
                                                                    "CAROLYNE TANG'ALI": "CAROLYNE NANGILA TANG'ALI" })
    loan_register['ROName Loans'] = loan_register['ROName Loans'].replace({
                                        r'.*AMBOK.*': 'AMBOK ALICE ATIENO',
                                        r'.*NGARI.*': 'BENSON KAMAU NGARI',
                                        r'.*SIMON.*': 'SIMON NDIRANGU KIRAGU',
                                        r'.*KATIE.*': 'STEPHEN KATIE KAGWIRIA',
                                        r'.*KAVIVE.*': 'PHILIP KAVIVE'}, regex=True)
    
    # calculating correct days in arrears 
    def days_with_arrears(disbursement_date,loan,days,arrears,next_repay):
        disbursement_date = pd.to_datetime(disbursement_date)
        # Current datetime
        current_date = datetime.now()
        #     print(current_date)
        if days==0 and arrears>0:
            if loan == '0019020035':
                # Add 1 month 
                new_date = disbursement_date + relativedelta(months=1)
                return (current_date-new_date).days
            else: return(current_date-next_repay).days - 1
        else: return days  
    
    # cleaninr Days in arrears
    loan_register['Days_in_Arrears'] = loan_register.apply(lambda x: \
                                        days_with_arrears(x["Disbursement Date"],x['Loan No'],x["Days in Arrears"],\
                                                         x["Total In Arrears Loans"], x["Next Repayment Date"]), axis =1 )
    loan_register["Days in Arrears"] = loan_register["Days_in_Arrears"]
    loan_register.drop(columns=['Days_in_Arrears'], inplace=True)
    return loan_register


def upload_loan_register( df):    
    # Authorize client
    client = get_gspread_client(WRITE_SCOPES)
    sheet_id = "1DEKCaV3PaXcnAbK8ZoQa4ty7CzArmCG2zMsDrETVzYE"
    spreadsheet = client.open_by_key(sheet_id)
    
    # # Get "Loan Register" sheet data
    worksheet_id = 1503994147
    worksheet = spreadsheet.get_worksheet_by_id(worksheet_id)
    # optional: clear existing data
    worksheet.batch_clear(["A2:AC"])
    
    # write dataframe
    set_with_dataframe( worksheet, df, row=2, col=1, include_column_header=False,allow_formulas=False)
    

# ---CACHED PROCESSING OUTPUT FUNCTIONS ---
# This function performs all the heavy pandas calculations. 
# It is cached, so it only runs once when the data changes or cache expires.
@st.cache_data(ttl=3600)
def process_dashboard_data(df):
    data = {}

    # 1. Branch Statistics
    # Fix: include_groups=False excludes the grouping key from the lambda function input
    data["branch_stats"] = df.groupby("Branch Code", group_keys=False).apply(
        lambda g: pd.Series({
            "Customers": g['Member No'].nunique(),
            "Portfolio": g['Total Balance'].sum(),
            "Arrears Amount": g['Total In Arrears Loans'].sum(),
            "PAR": (g.loc[g['Days in Arrears'] > 0, 'Total Balance'].sum() / g['Total Balance'].sum())
        }),
        include_groups=False
    ).reset_index().sort_values("Portfolio", ascending=False)

    # 2. Ageing Summary
    # Fix: observed=False retains the current behavior for categorical columns
    category_arrears = pd.pivot_table(
        df, 
        columns='Category', 
        index='Branch Code', 
        values='Total Balance', 
        aggfunc='sum',
        fill_value=0, 
        margins=True, 
        margins_name="Total",
        observed=False
    )
    category_arrears["PAR"] = category_arrears.apply(lambda x: (x['Total']-x['Performing'])/x['Total'], axis=1)
    category_arrears.drop(columns="Performing", inplace=True)
    data["ageing_summary"] = category_arrears

    # 3. PAR Summary
    # Fix: observed=False retains the current behavior for categorical columns
    branch_par = pd.pivot_table(
        df, 
        index='Branch Code', 
        columns='Category', 
        values='Total Balance', 
        aggfunc='sum', 
        margins=True, 
        margins_name='Total',
        observed=False
    ).drop(columns='Performing').apply(lambda x: x / x['Total'], axis=1)
    
    cols_to_sum = branch_par.columns.difference(["Total"])
    branch_par["Total"] = branch_par[cols_to_sum].sum(axis=1)
    data["par_summary"] = branch_par

    # 4. RO Summary (for Arrears Page)
    # Fix: observed=False retains the current behavior
    ro_par = pd.pivot_table(
        df, 
        index='ROName Loans', 
        columns='Category', 
        values='Total Balance', 
        aggfunc='sum', 
        margins=True, 
        margins_name='Total',
        observed=False
    )
    ro_par["PAR"] = ro_par.apply(lambda x: (x['Total']-x['Performing'])/x['Total'], axis=1)
    ro_par = ro_par.drop(columns="Performing")[["Total",'PAR',"1-30", "31-60","61-90","91&Above"]]
    data["ro_summary"] = ro_par

    # 5. Arrears Aggregation (for Collections Page)
    data["arrears_agg"] = df.loc[df['Days in Arrears']>0,:].groupby("Member No").\
                agg({ 'Branch Code':'max', 'Member Name':'max' , 'Total Balance':"sum", 'Total In Arrears Loans': "sum",
                      'Days in Arrears': "max", 'ROName Loans': 'max'}).reset_index()
    
    return data

# ---------------- PAGE RENDERING FUNCTIONS ----------------

def render_file_upload():
    st.markdown(  """  <div style="
                    padding:5px;  border-radius:5px; background-color:#caeceb;  border-left:6px solid #1f77b4;  margin-bottom:5px;    ">
                    <h2 style="margin:0; color:#1f77b4;">  File and Data Uploads </h2>
                        </div>  """,    unsafe_allow_html=True)
    #----- Loan Register upload------
    st.markdown("#### Loan Register")
   # create uploader and limits the selectable files to specific extensionss
    uploaded_loan_register = st.file_uploader( "Upload file  (Excel files only)", type=['xlsx'],  key='loan register' )
    df_ls = None
    if uploaded_loan_register is not None:  
        # Handle Excel Files
        if uploaded_loan_register.name.endswith('.xlsx'):
            try:
                df_ls = read_excel_file(uploaded_loan_register)
                st.success("File Uploaded Successfully!")
            except Exception as e:
                st.error(f"Error reading file: {e}")
        else:
            st.error(f"Upload correct file formart")

    #----- Customer List upload------
    st.markdown("#### Customer List")
    # create uploader and limits the selectable files to specific extensions
    uploaded_customer_list = st.file_uploader( "Upload file  (Excel files only)", type=['xlsx'], key='customer list' )
    df_cl = None
    if uploaded_customer_list is not None:  
        # Handle Excel Files
        if uploaded_customer_list.name.endswith('.xlsx'):
            try:
                df_cl =read_excel_file(uploaded_customer_list)
                st.success("File Uploaded Successfully!")
            except Exception as e:
                st.error(f"Error reading file: {e}")
        else:
            st.error(f"Upload correct file formart")

    #----- Update/ write google sheet------
    st.markdown( """ <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}
            </style> """, unsafe_allow_html=True )
    with st.expander("Update Loan Register", icon="📋"):
        if (uploaded_customer_list is not None) and (uploaded_loan_register is not None):
            if st.button("🔄 Data Upload", type="primary"):
                try: 
                    with st.spinner("Cleaning Data......"):
                        df = clean_loan_register(df_ls,df_cl)
                    with st.spinner("Uploading Data......"):
                        upload_loan_register(df)
                    st.success("Data Upload Successful")
                except Exception as e:
                    st.error(f"Error updating Loan Register: {e}")
            else: st.error(f"Click Button to updating Loan Register")
        else: 
            if (uploaded_loan_register is None) : st.error(f"Upload Loan Register file to Complete this operation")
            elif (uploaded_customer_list is None) : st.error(f"Upload Customer List file to Complete this operation")


def render_overview(df, processed_data):
    st.markdown( """ <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}
            </style> """, unsafe_allow_html=True )
    
    col1, col2 = st.columns([5,2], vertical_alignment="center")
    with col1:
        st.markdown(  """  <div style="
        padding:5px;  border-radius:5px; background-color:#caeceb;  border-left:6px solid #1f77b4;  margin-bottom:5px;    ">
                <h2 style="margin:0; color:#1f77b4;">  PORTFOLIO REPORT SUMMARY </h2>   </div>  """,    unsafe_allow_html=True)
    with col2:
        if st.button("🔄 Loan Register", type="primary"):
            load_loan_register.clear()
            process_dashboard_data.clear()
            st.rerun()

    # --- KPIs ---
    total_portfolio = df['Total Balance'].sum()
    total_arrears = df['Total In Arrears Loans'].sum()
    non_performing = df.loc[df['Days in Arrears']>0, 'Total Balance'].sum()
    par = non_performing / total_portfolio if total_portfolio else 0

    col1, col2, col3 = st.columns(3)
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

    # --- Branch Statistics (Using Cached Data) ---
    st.markdown("#### Branch Summary")
    branch_stats = processed_data["branch_stats"]
    styled = (
        branch_stats.style
        .format({ "Customers": "{:,}", "Portfolio": "{:,.0f}", "Arrears Amount": "{:,.0f}", "PAR": "{:.2%}" })
        .map(par_color, subset=["PAR"]) 
    )
    st.dataframe(styled.hide(axis="index"), use_container_width=True, height=280)

    # --- RO Summary (Using Cached Data) ---
    st.markdown("#### RO Summary")
    ro_summary = processed_data["ro_summary"]
    styled2 = (
        ro_summary.style
        .format({ "PAR": "{:.2%}", "1-30": "{:,.0f}", "31-60": "{:,.0f}", "61-90": "{:,.0f}","91&Above": "{:,.0f}","Total": "{:,.0f}" })
        .map(par_color, subset=["PAR"]) )
    st.dataframe(styled2.hide(axis="index"), use_container_width=True, height=600)

     # --- Loan Register Data ---
    st.markdown("#### Portfolio Data")
    with st.expander("Preview Loan Register", icon="📋"):
        # st.dataframe(df)
        col1, col2 = st.columns(2)
        with col1:
            branch_filter = st.selectbox("Branch Code", options=["All"] + list(df["Branch Code"].dropna().unique()), key="overview_arr_br")
            if  branch_filter=="All": branch_filter = df["Branch Code"].unique() 
            else: branch_filter = [branch_filter]
        with col2:
            category_filter = st.selectbox("Category", options=["All"] + list(df["Category"].dropna().unique()), key="overview_arr_cat")
            if category_filter=="All": category_filter = df["Category"].unique() 
            else: category_filter = [category_filter]

        filtered_df = df[(df["Category"].isin(category_filter)) &
                          (df["Branch Code"].isin(branch_filter))].sort_values(by=['Days in Arrears','Member Name'], ascending=True)
        st.dataframe(filtered_df.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}" }))

def render_arrears(df, processed_data):
    st.markdown(  """  <div style="
            padding:5px 10px;  border-radius:5px; background-color:#caeceb;  border-left:6px solid #1f77b4;  margin-bottom:2px; width:500px;   ">
                    <h2 style="margin:0; color:#1f77b4;"> 📉  ARREARS TRACKER  </h2>
                        </div>  """,    unsafe_allow_html=True)

        # --- Ageing Summary (Using Cached Data) ---
    st.markdown("#### Ageing Summary")
    category_arrears = processed_data["ageing_summary"]
    styled = (
        category_arrears.style
        .format({ "1-30": "{:,.0f}", "31-60": "{:,.0f}", "61-90": "{:,.0f}","91&Above": "{:,.0f}","Total": "{:,.0f}", "PAR": "{:.2%}" })
        .map(par_color, subset=["PAR"]) 
    )
    st.dataframe(styled.hide(axis="index"), use_container_width=True, height=310)

    # --- PAR Summary (Using Cached Data) ---
    st.markdown("#### PAR Summary")
    branch_par = processed_data["par_summary"]
    styled1 = (
        branch_par.style
        .format({ "1-30": "{:.2%}", "31-60": "{:.2%}", "61-90": "{:.2%}","91&Above": "{:.2%}","Total": "{:.2%}" })
        .map(par_color, subset=["Total"]) )
    st.dataframe(styled1.hide(axis="index"), use_container_width=True, height=310)  

    st.markdown("#### Detailed Arrears Data")
    dff = df.loc[df['Days in Arrears']>0]
    with st.expander("Preview Data",icon="📋"):
        col1, col2, col3 = st.columns(3)
        with col1:
            ro_name = st.selectbox("RO Name", options=["All"] + list(df['ROName Loans'].dropna().unique()), key="arr_ro")
            if ro_name=="All": ro_name = df['ROName Loans'].unique() 
            else: ro_name = [ro_name]
        with col2:
            category_filter = st.selectbox("Category", options=["All"] + list(df["Category"].dropna().unique()), key="arrears_arr_cat")
            if category_filter=="All": category_filter = df["Category"].unique() 
            else: category_filter = [category_filter]
        with col3:
            branch_filter = st.selectbox("Branch Code", options=["All"] + list(df["Branch Code"].dropna().unique()), key="arrears_arr_br")
            if  branch_filter=="All": branch_filter = df["Branch Code"].unique() 
            else: branch_filter = [branch_filter]
        
        filtered_df = dff[(dff['ROName Loans'].isin(ro_name)) &
                          (dff["Category"].isin(category_filter)) &
                          (dff["Branch Code"].isin(branch_filter))].sort_values(by=['Days in Arrears','Member Name'], ascending=True)
        st.dataframe(filtered_df.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}" }))

def render_collections(df, arrears_agg):
    # header --title
    st.markdown(  """  <div style="
            padding:5px 10px;  border-radius:5px; background-color:#caeceb;  border-left:6px solid #1f77b4;  margin-bottom:2px; width:700px;   ">
                    <h2 style="margin:0; color:#1f77b4;">📋  COLLECTIONS AND DEMANDS </h2>
                        </div>  """,    unsafe_allow_html=True)
    # checking collections data in cache
    if "coll_data" not in st.session_state:
        st.session_state.coll_data = pd.DataFrame()

    st.markdown( """ <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}
            </style> """, unsafe_allow_html=True )
    
    if st.button("Fetch Collection Remarks", type="primary"):
        with st.spinner("Loading Collections Data...."):
            st.session_state.coll_data = load_collections_data(creds)
    
    coll_data = st.session_state.coll_data    

    # Customer Search
    cust_cols = ['Member Name', 'Loan No','Loan Type', 'Total Balance', 'Total In Arrears Loans', 'Days in Arrears']   
    search_val = st.text_input("Search File No", key="file_search")
    
    if search_val: 
        search_val = str(search_val.rjust(4,"0"))
        # Note: We copy df to avoid SettingWithCopyWarning on the main df if we modified it, 
        # though here we just read.
        if search_val not in df['Member No'].astype(str).unique():
            st.markdown("""<div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White;">No such Active customer found</div>""", unsafe_allow_html=True)
            return
        else: 
            customer_details = df.loc[df['Member No'].astype(str) == search_val, cust_cols]
            styled_table = (customer_details.style.format({ "Total Balance": "{:,.0f}", "Total In Arrears Loans": "{:,.0f}" }))
            st.dataframe(styled_table)
    else:  
        st.write("Enter file No to preview remarks")
            
    # --- Filter Collections Data -----
    if coll_data.empty:
        st.markdown("""<div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White; font-weight: bold;">No data found ....Click Fetch remarks</div>""", unsafe_allow_html=True)
        return

    with st.expander("Preview Customer Print Out",icon="📋"):
        if search_val:
            filtered_df = coll_data[(coll_data['File Number'].astype(str) == search_val)]
            if filtered_df.empty: 
                st.warning("No remarks found")
            else:   
                st.dataframe(filtered_df)
        else: 
            st.write("Enter file No to preview remarks")
        
    st.markdown("#### RO Collections Summary")
    
    # Use the pre-calculated arrears_agg from cache
    arrears_data = arrears_agg.copy()
    arrears_data['Member No'] = pd.to_numeric(arrears_data['Member No'], errors='coerce')
        
    # Process remarks (must run if coll_data changes, but coll_data is in session_state)
    remarks = coll_data.drop_duplicates(subset='File Number', keep='first')\
                       .loc[:,['File Number',"Timestamp",'Outcomes','Officer Comments']].copy()
    remarks['File Number'] = pd.to_numeric(remarks['File Number'], errors='coerce')
        
    # Merge (Fast now because arrears_data is pre-aggregated)
    customer_data = arrears_data.merge(remarks, how="left", left_on='Member No', right_on='File Number')\
                                    .drop(columns='File Number').fillna(" ")
    customer_data['Timestamp'] = pd.to_datetime(customer_data['Timestamp'], errors='coerce')\
                                    .dt.strftime('%d-%B-%Y').fillna(" ")
    
    with st.expander("Preview Summary",icon="📋"):
        col1, col2 = st.columns(2)
        with col1:
            ro_name = st.selectbox("RO Name", options=["All"] + sorted(list(customer_data['ROName Loans'].dropna().unique())), key="ro_search")
            if ro_name=="All": ro_name = customer_data['ROName Loans'].unique() 
            else: ro_name = [ro_name]
        with col2:
            branch_filter = st.selectbox("Branch Code", options=["All"] + list(customer_data["Branch Code"].dropna().unique()), key="branch_search")
            if  branch_filter=="All": branch_filter = customer_data["Branch Code"].unique() 
            else: branch_filter = [branch_filter]
        
        cols = ['Member Name', 'Total Balance', 'Total In Arrears Loans', 'Days in Arrears', 'Timestamp', 'Outcomes', 'Officer Comments']
        filtered_data = customer_data.loc[(customer_data['ROName Loans'].isin(ro_name)) &
                                          (customer_data['Branch Code'].isin(branch_filter)), cols]\
                                    .sort_values(by='Days in Arrears', ascending=True)
        st.dataframe(filtered_data.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}"}))

def render_ro_page(name,df,arrears_agg,dis_tat):
    # st.header(f"Officer Statistics - {name.title()}") #f0f2f6
    st.markdown(    f""" <div style="  padding:5px;  border-radius:5px; background-color:#caeceb;  border-left:6px solid #1f77b4; margin-bottom:5px;    ">
        <h2 style="margin:0; color:#1f77b4;">   👤 Officer Statistics — {name.upper()}  </h2>  </div>  """,    unsafe_allow_html=True)

    # if passed name is not an ROs name the create a dropdown
    ro_names = sorted(list(df['ROName Loans'].dropna().unique()))
    if name not in ro_names:
        col1,  = st.columns(1)
        with col1:
            ro_name = st.selectbox("RO Name", options=[" "] +ro_names , key="ro_dropdown")
            if ro_name!=" ": name = ro_name
    
        # --- KPIs ---
    customers_count =  df.loc[df['ROName Loans'] == name, 'Member No'].nunique()
    total_portfolio = df.loc[df['ROName Loans'] == name, 'Total Balance'].sum()
    total_arrears = df.loc[df['ROName Loans'] == name, 'Total In Arrears Loans'].sum()
    non_performing = df.loc[ ((df['ROName Loans'] == name) & (df['Days in Arrears']>0)), 'Total Balance'].sum()
    par = non_performing / total_portfolio if total_portfolio else 0
    total_disbursments =  dis_tat.loc[ dis_tat["ROName"]==name ,"New Money"].sum()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown("##### 👤Customers")
        st.markdown(f"#### {customers_count:,.0f}")
    with col2:
        st.markdown("##### 💼Portfolio")
        st.markdown(f"#### {total_portfolio:,.0f}")
    with col3:
        st.markdown("##### 💰Disbursment")
        st.markdown(f"#### {total_disbursments:,.0f}")
    with col4:
        st.markdown("##### ⚠️ Arrears")
        st.markdown(f"#### {total_arrears:,.0f}")
        st.caption(f"Arrear {total_arrears/total_portfolio:.1%} of loan book")
    with col5:
        st.markdown("##### 📉 PAR")
        st.markdown(f"#### {par:.2%}")
        st.caption(f"Non-Performing loan book: {non_performing/1_000_000:,.2f}M ")

    st.markdown("#### RO Loan Book Data")
    name = [name]
    # ----- Preview Disbursements ----
    with st.expander("Preview Disbursements",icon="📋"):
        filtered_df = dis_tat.loc[ dis_tat["ROName"].isin(name),:]\
                        .sort_values(by=['Date'], ascending=[True] ).reset_index(drop=True)
        st.dataframe(filtered_df.style.format({ "New Money": "{:,.2f}"}))
    
    # ----- Preview LS ----
    with st.expander("Preview Loan Register",icon="📋"):
        # st.dataframe(df)
        col1, = st.columns(1)
        with col1:
            category_filter = st.selectbox("Category", options=["All"] + list(df["Category"].dropna().unique()), key="ro_arr_cat")
            if category_filter=="All": category_filter = df["Category"].unique() 
            else: category_filter = [category_filter]

        filtered_df = df.loc[ ((df["Category"].isin(category_filter)) & (df['ROName Loans'].isin(name) )),: ]\
                        .sort_values(by=['Days in Arrears','Member Name'], ascending=[True, True] ).reset_index(drop=True)
        st.dataframe(filtered_df.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}" }))

     # ----- Preview Collections ----
    st.markdown( """ <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}
            </style> """, unsafe_allow_html=True )
    
    col1, col2 = st.columns([5,2], vertical_alignment="center")
    with col1: st.markdown("#### Collections Data")
    with col2:
        if "coll_data" not in st.session_state:
            st.session_state.coll_data = pd.DataFrame()        
        if st.button("Fetch Collection Remarks", type="primary"):
            with st.spinner("Loading Collections Data...."):
                st.session_state.coll_data = load_collections_data(creds)
    
    coll_data = st.session_state.coll_data 

    if coll_data.empty:
        st.markdown("""<div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White; font-weight: bold;">No data found ....Click Fetch Collections </div>""", unsafe_allow_html=True)
        return
    
    # Use the pre-calculated arrears_agg from cache
    arrears_data = arrears_agg.copy()
    arrears_data['Member No'] = pd.to_numeric(arrears_data['Member No'], errors='coerce')
        
    # Process remarks (must run if coll_data changes, but coll_data is in session_state)
    remarks = coll_data.drop_duplicates(subset='File Number', keep='first')\
                       .loc[:,['File Number',"Timestamp",'Outcomes','Officer Comments']].copy()
    remarks['File Number'] = pd.to_numeric(remarks['File Number'], errors='coerce')
        
    # Merge (Fast now because arrears_data is pre-aggregated)
    customer_data = arrears_data.merge(remarks, how="left", left_on='Member No', right_on='File Number')\
                                    .drop(columns='File Number').fillna(" ")
    customer_data['Timestamp'] = pd.to_datetime(customer_data['Timestamp'], errors='coerce')\
                                    .dt.strftime('%d-%B-%Y').fillna(" ")
    
    with st.expander("Preview Summary",icon="📋"):        
        cols = ['Member Name', 'Total Balance', 'Total In Arrears Loans', 'Days in Arrears', 'Timestamp', 'Outcomes', 'Officer Comments']
        filtered_data = customer_data.loc[customer_data['ROName Loans'].isin(name) , cols]\
                                    .sort_values(by='Days in Arrears', ascending=True).reset_index(drop=True)
        st.dataframe(filtered_data.style.format({ "Total Balance": "{:,.2f}","Total In Arrears Loans": "{:,.2f}"}))

    
    #----- Customer Print out-------
    st.markdown("#### Customer Print Out")
    cust_cols = ['Member Name', 'Loan No','Loan Type', 'Total Balance', 'Total In Arrears Loans', 'Days in Arrears']   
    search_val = st.text_input("Search File No", key="file_search")
    
    if search_val: 
        search_val = str(search_val.rjust(4,"0"))
        # Note: We copy df to avoid SettingWithCopyWarning on the main df if we modified it, 
        # though here we just read.
        if search_val not in df['Member No'].astype(str).unique():
            st.markdown("""<div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White;">No such Active customer found</div>""", unsafe_allow_html=True)
            return
        else: 
            customer_details = df.loc[df['Member No'].astype(str) == search_val, cust_cols]
            styled_table = (customer_details.style.format({ "Total Balance": "{:,.0f}", "Total In Arrears Loans": "{:,.0f}" }))
            st.dataframe(styled_table)
    else:  
        st.write("Enter file No to preview remarks")
            
    # --- Filter Collections Data -----
    if coll_data.empty:
        st.markdown("""<div style="width: 40%;background-color:#eb8888; padding:15px; border-radius:5px; color:White; font-weight: bold;">No data found ....Click Fetch remarks</div>""", unsafe_allow_html=True)
        return

    with st.expander("Preview Customer Print Out",icon="📋"):
        if search_val:
            filtered_df = coll_data[(coll_data['File Number'].astype(str) == search_val)]
            if filtered_df.empty: 
                st.warning("No remarks found")
            else:   
                st.dataframe(filtered_df)
        else: 
            st.write("Enter file No to preview remarks")


# ---------------- MAIN APP LOGIC ----------------
def get_current_user():
    user = getattr(st, "user", None)
    if user and user.is_logged_in:
        return user
    return None

def get_user_role():
    user = get_current_user()
    if not user:
        return None, None 
    user_roles_mapping = st.secrets.get("user_roles", {})
    user_entry = user_roles_mapping.get(user.email)  
    if not user_entry:
        return None, None        
    role = user_entry.get("role")
    name = user_entry.get("name")
    return role, name

def render_sidebar(name, role, display_options):
    with st.sidebar:
        st.title(f"👤 {role.title() if role else 'Unknown'} Role")
        st.caption(f"User: {name.title() if name else 'Unknown User'}")
        default_index = 0
        if "selected_page" in st.session_state:
            if st.session_state.selected_page in display_options:
                default_index = display_options.index(st.session_state.selected_page)

        selected = st.radio(label="Menu", options=display_options, index=default_index, 
                            key="selected_page",label_visibility="collapsed" )
        st.divider()
        if st.button("Logout", use_container_width=True):
            st.logout()
    return selected


def main():
    user = get_current_user()
    if not user:
        st.title("Welcome to the Portfolio Dashboard")
        st.info("Please use the sidebar to log in and access your reports.")
        with st.sidebar:
            st.title("Authentication")
            st.write("Click below to start:")
            st.markdown(""" <style>   div.stButton > button[kind="primary"] {background-color: #a1b586; color: white;}</style> """, unsafe_allow_html=True)
            if st.button("Log in with Google", type="primary", use_container_width=True):
                st.login("google")
        return

    role, name = get_user_role()
    if not role:
        st.error("Access Denied: Your email is not registered in the system.")
        if st.sidebar.button("Logout"): st.logout()
        return   
    PAGE_ACCESS = {
        "teams": ["ro_stats"],
        "credit": [ "arrears", "collections"],
        "finance":["data_upload" ],
        "manager": [ "arrears", "collections","ro_stats"],
        "HOD": ["data_upload", "overview", "arrears"],
        "admin": ["data_upload", "overview", "arrears", "collections", "ro_stats"]
    }
    PAGE_MENU = {
        "data_upload":"📂 Data Upload",
        "overview": "📊 Portfolio ",
        "arrears": "Arrears Tracker",
        "collections": "Collections",
        "ro_stats": "RO Statistics"
    }
    allowed_keys = PAGE_ACCESS.get(role, [])
    if not allowed_keys:
        st.error(f"Access Denied: No permissions for role '{role}'.")
        return
    display_options = [PAGE_MENU[k] for k in allowed_keys]
    selection = render_sidebar(name, role, display_options)
    if not selection: return
        
    # --- LOAD DATA ---
    try:
        # 1. Load Raw Data (Cached)
        if "loan_df" not in st.session_state:
            with st.spinner("Refreshing Portfolio Data..."):
                st.session_state.loan_df = load_loan_register()
        df = st.session_state.loan_df
        # 2. Load Disbursements (Cached)
        if "disburse_df" not in st.session_state:
            with st.spinner("Refreshing Disbursement Data..."):
                st.session_state.disburse_df = load_disbursements(df)
       # This creates all the pivot tables and groupby objects instantly from cache       
        if "processed_data" not in st.session_state:
            with st.spinner("Processing Dashboard Data..."):
                st.session_state.processed_data = process_dashboard_data(st.session_state.loan_df)    
        dis_tat = st.session_state.disburse_df
        processed_data = st.session_state.processed_data
    except Exception as e:
        st.error(f"Login successful, but failed to reach Google Sheets: {e}")
        return

    # --- PAGE ROUTING ----
    page_key = [k for k, v in PAGE_MENU.items() if v == selection][0]

    if page_key == "overview":
        render_overview(df, processed_data)
    elif page_key == "arrears":
        render_arrears(df, processed_data)
    elif page_key == "collections":
        render_collections(df, processed_data["arrears_agg"])
    elif page_key == "ro_stats":
        render_ro_page(name, df,processed_data["arrears_agg"],dis_tat)
    elif page_key == 'data_upload':
        render_file_upload()

if __name__ == "__main__":
    main()
