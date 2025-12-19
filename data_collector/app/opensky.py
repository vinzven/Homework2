import requests
from datetime import datetime, timedelta
from time import time

from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException


class OpenSkyClient:
    def __init__(self, client_id=None, client_secret=None):
        self.api_url = "https://opensky-network.org/api"
        self.auth_url = (
            "https://auth.opensky-network.org/auth/realms/"
            "opensky-network/protocol/openid-connect/token"
        )

        self.client_id = "cicciopasticcio89-api-client"
        self.client_secret = "jsDRqroADX6W98izOo6HF0d3b27ZpYUv"
        self.token = None
        self.token_expires_at = 0

        if not self.client_id or not self.client_secret:
            raise RuntimeError("OpenSky credentials missing. Set CLIENT_ID and CLIENT_SECRET")

        # 🔴 CIRCUIT BREAKER (fornito dal docente)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30
        )

        self.authenticate()

    # --------------------------------------------------
    # AUTHENTICATION
    # --------------------------------------------------
    def authenticate(self):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        r = requests.post(self.auth_url, data=payload, timeout=15)
        r.raise_for_status()

        j = r.json()
        self.token = j.get("access_token")
        expires_in = j.get("expires_in", 3600)
        self.token_expires_at = int(time()) + int(expires_in) - 30

        print(f"[OpenSky] Token ottenuto, scade in {expires_in} secondi", flush=True)

    def _ensure_token(self):
        if not self.token or int(time()) >= self.token_expires_at:
            self.authenticate()

    def get_headers(self):
        self._ensure_token()
        return {"Authorization": f"Bearer {self.token}"}

    # --------------------------------------------------
    # RAW CALL (può fallire → exception)
    # --------------------------------------------------
    def _call_api_raw(self, path, params=None):
        url = f"{self.api_url}{path}"

        r = requests.get(
            url,
            params=params,
            headers=self.get_headers(),
            timeout=30
        )

        if r.status_code == 200:
            return r.json()

        elif r.status_code == 401:
            # Retry una sola volta
            self.authenticate()
            r = requests.get(
                url,
                params=params,
                headers=self.get_headers(),
                timeout=30
            )
            if r.status_code == 200:
                return r.json()
            raise Exception("Unauthorized after token refresh")

        elif r.status_code == 404:
            return []

        else:
            raise Exception(f"OpenSky API error {r.status_code}: {r.text}")

    # --------------------------------------------------
    # PROTECTED CALL (Circuit Breaker)
    # --------------------------------------------------
    def _call_api(self, path, params=None):
        return self.circuit_breaker.call(
            self._call_api_raw,
            path,
            params
        )

    # --------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------
    def get_departures(self, airport_icao, hours_back=24):
        end = int(datetime.utcnow().timestamp())
        begin = int((datetime.utcnow() - timedelta(hours=hours_back)).timestamp())

        params = {
            "airport": airport_icao,
            "begin": begin,
            "end": end
        }

        return self._call_api("/flights/departure", params)

    def get_arrivals(self, airport_icao, hours_back=24):
        end = int(datetime.utcnow().timestamp())
        begin = int((datetime.utcnow() - timedelta(hours=hours_back)).timestamp())

        params = {
            "airport": airport_icao,
            "begin": begin,
            "end": end
        }

        return self._call_api("/flights/arrival", params)

    def get_flights_for_airport(self, airport_icao, hours_back=24):
        try:
            departures = self.get_departures(airport_icao, hours_back)
            arrivals = self.get_arrivals(airport_icao, hours_back)

            return {
                "departures": departures or [],
                "arrivals": arrivals or [],
                "total": len(departures or []) + len(arrivals or [])
            }

        except CircuitBreakerOpenException:
            # Fallback quando il circuito è OPEN
            print("[OpenSky] Circuit breaker OPEN → fallback", flush=True)
            return {
                "departures": [],
                "arrivals": [],
                "total": 0
            }
