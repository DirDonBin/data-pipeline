
using Microsoft.AspNetCore.Components;
using WebClient.Utils;

namespace WebClient.Components.Layout
{
    public partial class MainLayout
    {
        [Inject] private IAuthService _authService { get; set; } = default!;
        [Inject] private NavigationManager _navigation { get; set; } = default!;

        protected override async Task OnInitializedAsync()
        {
            await CheckAuth();
        }

        public async Task CheckAuth()
        {            
            if (!await _authService.IsAuthenticatedAsync())
                _navigation.NavigateTo("/login");
        }
    }
}
