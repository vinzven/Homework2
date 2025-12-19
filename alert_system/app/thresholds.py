def check_threshold(total_flights, high_value=None, low_value=None):
    """
    Controlla se il numero di voli supera o scende sotto le soglie.
    Ritorna None se nessuna soglia superata, altrimenti una stringa con il messaggio.
    """
    if high_value is not None and total_flights > high_value:
        return f"Superata soglia superiore ({high_value})"
    if low_value is not None and total_flights < low_value:
        return f"Sotto soglia inferiore ({low_value})"
    return None
