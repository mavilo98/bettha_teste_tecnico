# Definição dos schemas para cada tabela
schemas = {
    'heroes': {
        'tipo_carga' : 'dim',
        'hero_id': 'Integer',
        'name': 'String',
        'email': 'String',
        'birthdate': 'Date',
        'birthplace': 'String',
        'workplace': 'String',
        'gender': 'String',
        'gender_identity': 'String',
        'sexual_orientation': 'String',
        'ethnicity': 'String'
    },
    'missions': {
        'tipo_carga' : 'fato',
        'mission_id': 'Integer',
        'hero_id': 'Integer',
        'mission_name': 'String',
        'is_squad_leader': 'Boolean',
        'start_date': 'Date',
        'end_date': 'Date',
        'is_ongoing': 'Boolean',
        'most_required_skill_id': 'Integer',
        'target_villain_id': 'Integer'
    },
    'skills': {
        'tipo_carga' : 'dim',
        'skill_id': 'Integer',
        'hero_id': 'Integer',
        'super_power_id': 'Integer',
        'strength': 'Integer'
    },
    'super_powers': {
        'tipo_carga' : 'dim',
        'super_power_id': 'Integer',
        'super_power_name': 'String'
    },
    'villains': {
        'tipo_carga' : 'dim',
        'villain_id': 'Integer',
        'name': 'String',
        'mortal_enemy_hero_id': 'Integer'
    }
}