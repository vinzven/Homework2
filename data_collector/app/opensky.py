import requests
from datetime import datetime, timedelta
from time import time

class OpenSkyClient:
    def __init__(self, client_id=None, client_secret=None):
        self.api_url = "https://opensky-network.org/api"
        self.auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        # Credenziali hardcoded
        self.client_id = client_id
        self.client_secret = client_secret 
        self.token = None
        self.token_expires_at = 0

        if not self.client_id or not self.client_secret:
            raise RuntimeError("OpenSky credentials missing. Set CLIENT_ID and CLIENT_SECRET")
        
        self.authenticate()

    def authenticate(self):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        try:
            r = requests.post(self.auth_url, data=payload, timeout=15)
            r.raise_for_status()
            j = r.json()
            self.token = j.get("access_token")
            expires_in = j.get("expires_in", 3600)
            self.token_expires_at = int(time()) + int(expires_in) - 30
            print(f"[OpenSky] Token ottenuto, scade in {expires_in} secondi", flush=True)
        except Exception as e:
            print(f"[OpenSky] Errore autenticazione: {e}", flush=True)
            self.token = None
            self.token_expires_at = 0

    def _ensure_token(self):
        if not self.token or int(time()) >= self.token_expires_at:
            self.authenticate()

    def get_headers(self):
        self._ensure_token()
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def _call_api(self, path, params=None):
        url = f"{self.api_url}{path}"
        try:
            r = requests.get(url, params=params, headers=self.get_headers(), timeout=30)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 401:
                # token scaduto
                self.authenticate()
                r = requests.get(url, params=params, headers=self.get_headers(), timeout=30)
                if r.status_code == 200:
                    return r.json()
                return []
            elif r.status_code == 404:
                return []
            else:
                print(f"[OpenSky] API error {r.status_code}: {r.text}", flush=True)
                return []
        except Exception as e:
            print(f"[OpenSky] Request error: {e}", flush=True)
            return []

    def get_departures(self, airport_icao, hours_back=24):
        end = int(datetime.utcnow().timestamp())
        begin = int((datetime.utcnow() - timedelta(hours=hours_back)).timestamp())
        params = {"airport": airport_icao, "begin": begin, "end": end}
        return self._call_api("/flights/departure", params=params)

    def get_arrivals(self, airport_icao, hours_back=24):
        end = int(datetime.utcnow().timestamp())
        begin = int((datetime.utcnow() - timedelta(hours=hours_back)).timestamp())
        params = {"airport": airport_icao, "begin": begin, "end": end}
        return self._call_api("/flights/arrival", params=params)

    def get_flights_for_airport(self, airport_icao, hours_back=24):
        departures = self.get_departures(airport_icao, hours_back)
        arrivals = self.get_arrivals(airport_icao, hours_back)
        total = len(departures or []) + len(arrivals or [])
        return {"departures": departures or [], "arrivals": arrivals or [], "total": total}
