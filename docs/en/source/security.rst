*******************
Security in Scorpio
*******************

Security Architecture
#####################

Scorpio Broker system will also be responsible for any of the Identity & authentication management security. This will include authentication & authorization of requests, users, role base protected resources to access in Scorpio Broker security realm. 

A new Authentication & Authorization service compliant to OAuth2.0 specs has been introduced that will provide the application layer security to the entire Scorpio Broker components & services.

.. figure:: figures/security.png


Security - Functional Request Flow
##################################

1. Browser/end user sends a resource request which is protected to the Scorpio Broker system using the API gateway REST interface.

2. API Gateway checks if the security feature is enabled. 

 a. If yes then, it checks if the request is already authenticated and already has some existing session. 

 - If it does not find any session, then it forwards the request to Authentication & Authorization services. Or

 - If it finds any existing session than it reuses the same session for the authentication purpose and routes the request to the back-end resource service.

 b.If no security is enabled then, it bypasses security check and routes the request to the back-end resource service which is responsible to render the resource against the given request. 

3. Now when the request comes at Authentication & Authorization (Auth in short) service, it responds to the original requester i.e. user/browser with a login form to present their identity based on credentials it has been issued to access the resource.
 
4. So now the user submits the login form with its credential to Auth service. Auth services validate the user credentials based on its Account details and now responded with successful login auth code & also the redirect U to which the user can redirect to fetch its resource request. 

5. User/Browser now redirects at the redirect URL which is in our case is again the API gateway URL with the auth_code that it has received from the Auth service. 

6. Now API gateway again checks the session where it finds the existing session context but now this time since it receives the auth_code in the request so it uses that auth_code and requests the token from Auth service acting as a client on user’s behalf. Auth service based on auth code recognized that it is already logged-in validated user and reverts back with the access token to the API gateway.

7. The API gateway upon receiving the token (with in the same security session context), now relays/routes to the back-end resource service for the original requested resource/operation.

8. The back-end resource service is also enabled with security features (if not error will be thrown for the coming secure request). It receives the request and reads the security context out of it and now validates (based on some extracted info) the same with the Auth service to know if this is a valid token/request with the given privileges. Auth service response backs and back-end service decide now whether the local security configuration and the auth service-based access permissions are matching. 

9. If the access permissions/privileges are matched for the incoming request, then it responds back with the requested resources to the user/browser. In case, it does not match the security criteria than it reverts with the error message and reason why it's being denied.