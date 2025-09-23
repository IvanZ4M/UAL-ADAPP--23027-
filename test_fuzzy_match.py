from fuzzy_utils import fuzzy_match
from rapidfuzz import fuzz

query = {'nombre': 'Juan', 'apellido': 'Pérez', 'email': 'juan@email.com'}

choices = [
    {'nombre': 'Juan', 'apellido': 'Pérez', 'email': 'juan@email.com'},
    {'nombre': 'Juan', 'apellido': 'Pérez', 'email': 'juan.perez@email.com'},
    {'nombre': 'Juan', 'apellido': 'Perez', 'email': 'juan@email.com'},
    {'nombre': 'Juan', 'apellido': 'Pérez', 'email': 'juanperez@email.com'},
    {'nombre': 'Juanito', 'apellido': 'Pérez', 'email': 'juan@email.com'},
    {'nombre': 'Juan', 'apellido': 'Pérez', 'email': 'otro@email.com'},
]

pesos = {'nombre': 2, 'apellido': 3, 'email': 5}
campos = ['nombre', 'apellido', 'email']

print("="*60)
print("Comparando cada opción con el query:\n")
print(f"{'Opción':<7} {'Nombre':<12} {'Apellido':<12} {'Email':<25} {'Score':>6}")
print("-"*60)
for idx, choice in enumerate(choices):
    score_total = 0
    peso_total = 0
    for campo in campos:
        val_query = str(query.get(campo, "")).lower()
        val_choice = str(choice.get(campo, "")).lower()
        score_campo = fuzz.ratio(val_query, val_choice)
        score_total += score_campo * pesos[campo]
        peso_total += pesos[campo]
    score_final = score_total / peso_total if peso_total > 0 else 0
    print(f"{idx+1:<7} {choice['nombre']:<12} {choice['apellido']:<12} {choice['email']:<25} {score_final:6.2f}")
print("="*60)

result = fuzzy_match(query, choices, score_cutoff=0)
print("\nMejor coincidencia encontrada:")
print(f"Score: {result['score']:.2f}")
print("Match query:", result['match_query'])
print("Match result:", result['match_result'])