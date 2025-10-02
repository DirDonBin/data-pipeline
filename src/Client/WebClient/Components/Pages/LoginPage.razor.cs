using DataPipeline.ApiClient.Internal;
using Microsoft.AspNetCore.Components;
using WebClient.Utils;

namespace WebClient.Components.Pages
{
    public partial class LoginPage
    {
        [Inject] private IAuthService _auth { get; set; } = default!;
        [Inject] private NavigationManager _navigation { get; set; } = default!;

        private string? _login { get; set; }
        private string? _password { get; set; }

        private async Task OnClickLogin()
        {
            var res = await _auth.LoginAsync(new LoginDto
            {
                Email = _login,
                Password = _password
            });

            _navigation.NavigateTo("/");
        }
    }
}
