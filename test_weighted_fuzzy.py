import mysql.connector
from rapidfuzz import process, fuzz
import pandas as pd
import json
from datetime import datetime

def test_weighted_fuzzy_match():
    """
    Función de prueba para el sistema de coincidencia difusa con pesos.
    """
    # Datos de prueba
    source_records = [
        {'nombre': 'Michael', 'apellido': 'Jones', 'email': 'michael.jones@test.com'},
        {'nombre': 'Herbert', 'apellido': 'Davis', 'email': 'herbert.davis@test.com'},
        {'nombre': 'Christian', 'apellido': 'Martin', 'email': 'christian.martin@test.com'}
    ]
    
    target_records = [
        {'first_name': 'Michael', 'last_name': 'Robinson', 'email': 'michael.robinson@test.com'},
        {'first_name': 'Robert', 'last_name': 'Davis', 'email': 'robert.davis@test.com'},
        {'first_name': 'Christina', 'last_name': 'Miranda', 'email': 'christina.miranda@test.com'}
    ]
    
    # Definición de pesos
    weights = {
        'nombre': 2,    # Peso para el nombre
        'apellido': 3,  # Peso para el apellido
        'email': 5      # Peso para el email
    }
    
    # Mapeo de campos entre las tablas
    field_mappings = {
        'nombre': 'first_name',
        'apellido': 'last_name',
        'email': 'email'
    }
    
    def calculate_weighted_score(source_record, target_record):
        """
        Calcula la puntuación ponderada entre dos registros.
        """
        total_weight = sum(weights.values())
        weighted_scores = []
        
        for source_field, target_field in field_mappings.items():
            if source_field in source_record and target_field in target_record:
                source_value = str(source_record[source_field]).lower()
                target_value = str(target_record[target_field]).lower()
                
                # Calcular similitud usando diferentes métricas
                ratio_score = fuzz.ratio(source_value, target_value)
                token_sort_score = fuzz.token_sort_ratio(source_value, target_value)
                token_set_score = fuzz.token_set_ratio(source_value, target_value)
                
                # Usar el mejor score de las diferentes métricas
                best_field_score = max(ratio_score, token_sort_score, token_set_score)
                
                # Aplicar el peso del campo
                weight = weights.get(source_field, 1)
                weighted_scores.append((best_field_score * weight) / total_weight)
        
        return sum(weighted_scores) if weighted_scores else 0

    print("\nIniciando pruebas de coincidencia difusa con pesos...\n")
    print("Pesos utilizados:")
    print(f"Nombre: {weights['nombre']}")
    print(f"Apellido: {weights['apellido']}")
    print(f"Email: {weights['email']}\n")

    # Probar cada registro fuente contra todos los registros objetivo
    for source in source_records:
        print(f"\nBuscando coincidencias para: {source['nombre']} {source['apellido']}")
        best_match = None
        best_score = 0
        
        for target in target_records:
            score = calculate_weighted_score(source, target)
            print(f"Comparando con {target['first_name']} {target['last_name']}:")
            print(f"Score ponderado: {score:.2f}")
            
            if score > best_score:
                best_score = score
                best_match = target
        
        if best_match:
            print(f"\nMejor coincidencia encontrada:")
            print(f"Source: {source['nombre']} {source['apellido']}")
            print(f"Match: {best_match['first_name']} {best_match['last_name']}")
            print(f"Score final: {best_score:.2f}")
        print("-" * 50)

if __name__ == "__main__":
    test_weighted_fuzzy_match()