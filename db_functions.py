import sqlite3
from sqlite3 import Error
import pandas as pd
from tqdm.notebook import tqdm

def update_id_df(questions_df):
    
    querty = cur.execute('''select * from Questions''').fetchall()
    
    for i in tqdm(range(len(querty))):
        q_id, q_code = querty[i][0], querty[i][1]
        print(q_id, q_code)
        questions_df.iloc[questions_df.index[questions_df['Code'] == q_code][0]]['id'] = q_id
        

def update_id_dfa(questions_df, answers_df):
    
    for i in tqdm(range(answers_df.shape[0])):
    
        a_i, q_code = i, questions_df.iloc[i]['Question_code']
        q_id = questions_df.iloc[questions_df.index[questions_df['Code'] == q_code][0]]['id']

        answers_df.loc[i, 'Question_id'] = q_id
        
        
def write_df_q_to_db(questions_df):
    
    conn = sqlite3.connect(f'questions.db')
    cur = conn.cursor()
    
    n1, n2 = 0, questions_df.shape[0]
    for i in tqdm(range(n1, n2)):
        q_0 = questions_df.loc[i]
        code_1, text_1, sec_1, d_1 = q_0['Code'], q_0['Text'], q_0['Section_id'], 0
        cur.execute('''INSERT OR IGNORE INTO Questions (Code, Text, Section_id, Done)
                                VALUES (?,?,?,?)''', (code_1, text_1, sec_1, d_1,))
        conn.commit()
    
    conn.close()
        
def write_df_q_to_db(questions_df):
    
    conn = sqlite3.connect(f'questions.db')
    cur = conn.cursor()
    
    n1, n2 = 0, df_a.shape[0]
    for i in tqdm(range(n1, n2)):
        q_0 = df_a.loc[i]
        id_1, code_1, text_1, r_1 = q_0['Question_id'], q_0['Question_code'], q_0['Text'], q_0['Right'] if q_0['Right'] == True else False
        cur.execute('''INSERT OR IGNORE INTO Answers (Question_id, Question_code, Text, Right)
                                VALUES (?,?,?,?)''', (id_1, code_1, text_1, r_1,))
        conn.commit()
    
    conn.close()

def prepare_dfs():
    questions_df_columns = ['id', 'Code', 'Text', 'Section_id']
    answers_df_columns = ['Question_id', 'Question_code', 'Text', 'Right']

    questions_df = pd.DataFrame(columns=questions_df_columns)
    answers_df = pd.DataFrame(columns=answers_df_columns)
    
    return questions_df, answers_df
    
def show_para(doc, n1=0, n2=100):
    para_index = n1

    while para_index <= n2:
        para = doc.paragraphs[para_index]
        print(str(para_index) + '    ' + para.text)
        para_index += 1
        
        
#create new df with answers and questions
def doc_to_a_q_df(doc, n1=0, n2=100):
    
    number_of_para = len(doc.paragraphs)
    q_write = False
    a_write = False
    q_text = ''
    questions_df, answers_df = prepare_dfs()
    n2 = len(doc.paragraphs)

    for i in tqdm(range(n1, n2)):  #number_of_para

        para = doc.paragraphs[i]

        #check for empty para
        if para.text == '':
            continue

        #check for keyword
        code_str = para.runs[0].text
        if code_str == "Код":

            q_write, a_write = True, False
            q_code, s_id = para.runs[4].text, para.runs[4].text[0].split('.')[0]
            continue

        #check for additionals question rows
        if q_write is True:
            new_text = doc.paragraphs[i].text
            q_text = new_text if q_text == '' else q_text + ' \n' + new_text
            #print(q_text)

            if 'Ответ' in q_text:
                new_q_row = {'Code':q_code, 'Text':q_text, 'Section_id':s_id}
                questions_df = questions_df.append(new_q_row, ignore_index=True)
                q_text = ''
                q_write, a_write = False, True

        #check for additionals answer rows
        if a_write is True:
            para = doc.paragraphs[i]
            if para.text == '':
                continue

            a_text, a_is_right = para.text, para.runs[0].underline
            #print('it is a right answer ' if a_is_right==True else 'bad answer' , para.text)
            new_answer = {'Question_code':q_code, 'Text': a_text, 'Right': a_is_right}
            answers_df = answers_df.append(new_answer, ignore_index=True)
            
    return questions_df, answers_df
    


#create new db for questions and answers
def create_db():
    
    conn = sqlite3.connect(f'questions.db')
    cur = conn.cursor()
    
    cur.execute(f'''DROP TABLE Answers IF EXISTS ''')
    cur.execute(f'''CREATE TABLE Answers (
                    id	INTEGER UNIQUE,
                    Question_id	INTEGER,
                    Question_code	TEXT,
                    Text	TEXT,
                    Right	INTEGER,
                    PRIMARY KEY(id AUTOINCREMENT),
                    UNIQUE("Question_code","Text")
                )''')
    print('Table Answers has been created')
                
    cur.execute(f'''DROP TABLE Questions IF EXISTS ''')
    cur.execute(f'''CREATE TABLE Questions (
                    id	INTEGER UNIQUE,
                    Code	TEXT,
                    Text	TEXT,
                    Section_id	INTEGER,
                    Done	INTEGER,
                    PRIMARY KEY(id AUTOINCREMENT),
                    UNIQUE("Code","Text")
                )''')
    print('Table Questions has been created')
                
    cur.execute(f'''DROP TABLE Sections IF EXISTS ''')
    cur.execute(f'''CREATE TABLE Sections (
                    id	INTEGER UNIQUE,
                    Name	TEXT,
                    PRIMARY KEY(id AUTOINCREMENT)
                )''')
    print('Table Sections has been created')
                
    conn.close()
                
                