using DataPipeline.ApiClient.Internal;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace WebClient.Utils
{
    public interface IAuthService
    {
        Task<bool> LoginAsync(LoginDto loginModel);
        Task LogoutAsync();
        Task<bool> IsAuthenticatedAsync();
        Task<bool> ValidateTokenAsync();
    }

    public class AuthService : IAuthService
    {
        private readonly HttpClient _httpClient;
        private readonly IJSRuntime _jsRuntime;
        private readonly IConfiguration _configuration;
        private readonly IInternalApiClient _internalApiClient;
        private readonly CustomAuthStateProvider _authStateProvider;
        private readonly NavigationManager _navigation;

        public AuthService(HttpClient httpClient, IJSRuntime jsRuntime,
            IConfiguration configuration,
            IInternalApiClient internalApiClient,
            CustomAuthStateProvider authStateProvider, NavigationManager navigation)
        {
            _httpClient = httpClient;
            _jsRuntime = jsRuntime;
            _configuration = configuration;
            _internalApiClient = internalApiClient;
            _authStateProvider = authStateProvider;
            _navigation = navigation;
        }

        public async Task<bool> LoginAsync(LoginDto loginModel)
        {
            var response = await _internalApiClient.SignInAsync(loginModel);

            if (response != null)
            {
                await _jsRuntime.InvokeVoidAsync("localStorage.setItem", "authToken", response.Token);

                _authStateProvider.NotifyUserAuthentication(response.Token);

                return true;
            }

            return false;
        }

        public async Task<bool> ValidateTokenAsync()
        {
            try
            {
                var token = await _jsRuntime.InvokeAsync<string>("localStorage.getItem", "authToken");

                if (string.IsNullOrEmpty(token))
                    return false;

                // Проверяем валидность токена через API
                var response = await _httpClient.GetAsync("api/auth/validate");
                return response.IsSuccessStatusCode;
            }
            catch
            {
                return false;
            }
        }

        public async Task LogoutAsync()
        {
            await _jsRuntime.InvokeVoidAsync("localStorage.removeItem", "authToken");
            _authStateProvider.NotifyUserLogout();
            _navigation.NavigateTo("/login");
        }

        public async Task<bool> IsAuthenticatedAsync()
        {
            if (_authStateProvider.IsPrerendering())
                return true;

            try
            {
                var token = await _jsRuntime.InvokeAsync<string>("localStorage.getItem", "authToken");
                return !string.IsNullOrEmpty(token);
            }
            catch
            {
                return false;
            }
        }
    }
}
