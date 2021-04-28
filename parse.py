import datetime
import sqlite3
import re
from unidecode import unidecode
import dateparser
from utils import printProgressBar, levenshtein


def prepareDatabase(conn):
    cursor = conn.cursor()
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS parsed_content(name text, id text, link text, owner text, date date,'
        +' PRIMARY KEY(id))'
    )
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS parsed_content_type(content_id text, type text,'
        +' PRIMARY KEY(content_id, type))'
    )
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS parsed_content_course(content_id text, course_id text,'
        +' PRIMARY KEY(content_id, course_id))'
    )
    conn.commit()


def parseTypes(item):
    global noGuess

    typePatterns = [
        {'type': 'exam', 'pattern': '(examen(es)?)|(parcial(es)?)|(final(es)?)|((1|2)(p|c)|(p|c)(1|2))'},
        {'type': 'guide', 'pattern': '(problema(s)?)|(guia(s)?)|(practica(s)?)|(tp(e)?(s)?)|(tarea(s)?)'},
        {'type': 'exercise', 'pattern': '(ejercicio(s)?)'},
        {'type': 'project', 'pattern': '(proyecto(s)?|(lab(s)?)|(laboratorio(s)?))'},

        {'type': 'theory', 'pattern': '(nota(s)?)|(teori(c)?a(s)?)|(cuaderno(s)?)|(carpeta(s)?)|(apunte(s)?)|(clase(s)?)'
            +'|(unidad(es)?)|(u[0-9])|(presentacion(es)?)|(diapositiva(s)?)'},
        {'type': 'summary', 'pattern': '(formula(s)?)|(resumen(es)?)'},
        
        {'type': 'bibliography', 'pattern': '(texto(s)?)|(libro(s)?)|(capitulo(s)?)|(bibliografia(s)?)'},
        {'type': 'solution', 'pattern': '(solucion(es)?)|(respuesta(s)?)'},

        {'type': 'code', 'pattern': '(programa(s)?)|(codigo(s))'},
        {'type': 'suggestions', 'pattern': '(sugerencia(s)?)|(recomendacion(es)?)|(tip(s)?)|(clave(s)?)'},
        {'type': 'polls', 'pattern': 'encuesta(s)?'},
        {'type': 'miscellaneous', 'pattern': 'material util'},
    ]

    typePatternsEN = [
        {'type': 'exam', 'pattern': '(exam(s)?)|(partial(s)?)|(final(s)?)|((1|2)(p|c)|(p|c)(1|2))'},
        {'type': 'guide', 'pattern': '(problem(s)?)|(guide(s)?)|(practic(s)?)|(tp(e)?(s)?)|(homework(s)?)'},
        {'type': 'exercise', 'pattern': '(exercise(s)?)'},
        {'type': 'project', 'pattern': '(project(s)?|(lab(s)?)|(laboratory(s)?))'},

        {'type': 'theory', 'pattern': '(note(book)?(s)?)|(theor(i|y)(c)?(s)?)|(lesson(s)?)'
            +'|(unit(s)?)|(u[0-9])|(presentation(s)?)|(film(s)?)'},
        {'type': 'summary', 'pattern': '(formula(e|s)?)|(summar(i|y)(es)?)'},
        
        {'type': 'bibliography', 'pattern': '(text(s)?)|(book(s)?)|(chapter(s)?)|(bibliography)'},
        {'type': 'solution', 'pattern': '(solution(s)?)|(answers(s)?)'},

        {'type': 'code', 'pattern': '(program(s)?)|(code(s))'},
        {'type': 'suggestions', 'pattern': '(suggestion(s)?)|(tip(s)?)'},
        {'type': 'polls', 'pattern': 'poll(s)?'},
        {'type': 'miscellaneous', 'pattern': 'material util'},
    ]

    tokens = item['path'].split('/')[2:]

    item['types'] = []

    for typePattern in typePatterns:
        cleanTokens = [token.strip() for token in tokens]
        if any(re.search(typePattern['pattern'], token) != None for token in cleanTokens):
            item['types'].append(typePattern['type'])

    for typePattern in typePatternsEN:
        cleanTokens = [token.strip() for token in tokens]
        if any(re.search(typePattern['pattern'], token) != None for token in cleanTokens):
            item['types'].append(typePattern['type'])
    

    if len(item['types']) == 0:
        # print(f"No guess {item['path']}")
        noGuess += 1


def parseCourses(item, courses):
    global noGuess

    item['courses'] = []
    
    matches = re.findall('[0-9][0-9]\.[0-9][0-9]', item['path'])

    if len(matches) > 0:
        # Extracted code number
        item['courses'] += matches
    else:
        # Perform Levenshtein over course names
        tokens = item['path'].split('/')
        for token in tokens:
            for course in courses:
                if levenshtein(unidecode(course['name'].lower()), token.strip()) <= 3:
                    item['courses'].append(course['id'])

    if len(item['courses']) == 0:
        # print(f"No guess {item['path']}")
        noGuess += 1


def parseDate(item):
    global noGuess

    validTypes = ['exam']
    if not any(validType in item['types'] for validType in validTypes):
        return

    year = None
    month = None
    day = None
    
    tokens = item['path'].split('/')[3:]
    cleanTokens = [token.strip() for token in tokens]

    for token in cleanTokens:        
        
        # Try to get full date

        regex = '(([0-9][0-9](/|-))?[0-9][0-9](/|-)[0-9][0-9][0-9]?[0-9]?)|([0-9][0-9][0-9]?[0-9]?(/|-)[0-9][0-9]((/|-)[0-9][0-9])?)'
        match = re.search(regex, token)
        date = dateparser.parse(match.group(), languages=['es','en']) if match != None else None

        if date != None:
            year = date.year
            month = date.month
            day = date.day

        # Try to get parts of date

        if year == None:
            match = re.search('20[0-9][0-9]', token)
            year = match.group() if match != None else None

        if month == None:
            match = re.search('(1c|2c|c1|c2)', token)
            month = ("1" if match.group()=='1c' or match.group()=='c1' else "7") if match != None else None

    # If at least year is present, make a date

    if year != None:
        date = datetime.datetime(int(year), int(month if month != None else 1), int(day if day != None else 1))
        item['date'] = date
    else:
        # print(f"No guess {item['path']}")
        noGuess += 1

    return


noGuess = 0

def main():
    global noGuess

    conn = sqlite3.connect('out/data.db')

    prepareDatabase(conn)

    rawItems = conn.cursor().execute("SELECT path,id,link,owner FROM file WHERE type='file' LIMIT 1000").fetchall()
    parsedItems = []

    courses = conn.cursor().execute(f"SELECT id,name FROM course")
    courses = list(map(lambda c : {'id': c[0], 'name': c[1]}, courses))

    for i in range(0, len(rawItems)):
        printProgressBar(i+1, len(rawItems), prefix = 'Progress:', suffix = 'Complete', length = 50)

        rawItem = rawItems[i]

        parsedItem = {
            'path': unidecode(rawItem[0].lower()), 'id': rawItem[1], 'link': rawItem[2], 'owner': rawItem[3],
            'types': None, 'courses': None, 'date': None
        }

        parseTypes(parsedItem)

        parseCourses(parsedItem, courses)

        parseDate(parsedItem)

        parsedItems.append(parsedItem)

        itemName = parsedItem['path'].split('/')[-1]

        conn.cursor().execute(
            "INSERT INTO parsed_content(name,id,link,owner,date) VALUES(?,?,?,?,?)"
            +" ON CONFLICT(id) DO NOTHING",
            (itemName, parsedItem['id'], parsedItem['link'], parsedItem['owner'], parsedItem['date'])
        )

        for type in parsedItem['types']:
            conn.cursor().execute(
                "INSERT INTO parsed_content_type VALUES(?,?)"
                +" ON CONFLICT(content_id, type) DO NOTHING",
                (parsedItem['id'], type)
            )

        for course in parsedItem['courses']:
            conn.cursor().execute(
                "INSERT INTO parsed_content_course VALUES(?,?)"
                +" ON CONFLICT(content_id, course_id) DO NOTHING",
                (parsedItem['id'], course)
            )

    conn.commit()

    print(f"No guesses {noGuess}")


if __name__ == '__main__':
    main()