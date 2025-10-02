using Microsoft.AspNetCore.Components;
using WebClient.Utils;

namespace WebClient.Components.Layout
{
    public partial class NavMenu
    {
        [Inject] private IAuthService _auth { get; set; } = default!;

        public async Task LogoutAsync()
        {
            await _auth.LogoutAsync();
        }
    }
}
