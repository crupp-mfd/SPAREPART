(function initMFDAppsPortalConfig() {
  const current = window.__MFDAPPS_PORTAL_CONFIG__ || {};
  window.__MFDAPPS_PORTAL_CONFIG__ = {
    APP_OBJEKTSTRUKTUR_URL: current.APP_OBJEKTSTRUKTUR_URL || "",
    APP_BREMSENUMBAU_URL: current.APP_BREMSENUMBAU_URL || "",
    APP_TEILENUMMER_URL: current.APP_TEILENUMMER_URL || "",
    APP_WAGENSUCHE_URL: current.APP_WAGENSUCHE_URL || "",
    APP_RSRD_URL: current.APP_RSRD_URL || "",
    APP_GOLDENVIEW_URL: current.APP_GOLDENVIEW_URL || "",
    APP_SQL_API_URL: current.APP_SQL_API_URL || "",
    APP_MEHRKILOMETER_URL: current.APP_MEHRKILOMETER_URL || "",
  };
})();
