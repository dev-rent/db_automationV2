col_dict = {
    "enterprise": [
        'enterprise_id', 'ent_status', 'jur_sit', 'type_of_ent', 'jur_form',
        'jf_cac', 'start_date'
    ],
    "establishment": [
        "establishment_id", "start_date", "enterprise_id"
    ],
    "branch": [
        "branch_id", "start_date", "enterprise_id"
    ],
    "activity": [
        'entity_id', 'activity_group', 'nace_version',
        'nace_code', 'classification'
    ],
    "address": [
        'entity_id', 'type_of_address', 'country_nl',
        'country_fr', 'zipcode', 'municipality_nl', 'municipality_fr',
        'street_nl', 'street_fr', 'house_number', 'box_number', 'extra_info',
        'date_striking_off'
    ],
    "contact": [
        'entity_id', 'ent_contact', 'contact_type',
        'contact_value'
    ],
    "denomination": [
        'entity_id', 'code_language', 'denom_type', 'denomination'
    ],
    "codes": [
        'code', 'code_language', 'code_description'
    ]
}

map_dct = {
    "enterprise": "0",
    "establishment": "2",
    "branch": "9",
    "activity": "01",
    "address": "02",
    "contact": "03",
    "denomination": "04"
}

reverse_map_dct = {
    "0": "enterprise",
    "2": "establishment",
    "9": "branch",
    "01": "activity",
    "02": "address",
    "03": "contact",
    "04": "denomination",
}
