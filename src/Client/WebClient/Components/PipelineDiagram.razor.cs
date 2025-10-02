using Blazor.Diagrams;
using Blazor.Diagrams.Components;
using Blazor.Diagrams.Core.Anchors;
using Blazor.Diagrams.Core.Controls.Default;
using Blazor.Diagrams.Core.Geometry;
using Blazor.Diagrams.Core.Models;
using Blazor.Diagrams.Core.PathGenerators;
using Blazor.Diagrams.Core.Routers;
using Blazor.Diagrams.Options;
using WebClient.Models;

namespace WebClient.Components
{
    public partial class PipelineDiagram
    {
        private BlazorDiagram Diagram { get; set; } = null!;

        protected override void OnInitialized()
        {
            var options = new BlazorDiagramOptions
            {
                AllowMultiSelection = true,
                Zoom =
            {
                Enabled = false,
            },
                Links =
            {
                DefaultRouter = new NormalRouter(),
                DefaultPathGenerator = new SmoothPathGenerator(),
                EnableSnapping = true,
                DefaultColor= "#64EDF0",

            },
                GridSnapToCenter = true,
                GridSize = 10
            };

            Diagram = new BlazorDiagram(options);

            Diagram.RegisterComponent<DiagramWidgetNode, DiagramWidget>();
            var firstNode = Diagram.Nodes.Add(new DiagramWidgetNode(position: new Point(50, 50))
            {
                Name = "Node 1",
                Content = @"{
                   ""lorem_text"": ""provident aut et"", ""provident aut et"" ""provident aut et""
                   ""ipv4"": ""227.113.145.33"",
                   ""username"": ""Eloise.Carroll"",
                   ""password"": ""MLAPh4R57dLteeg"",
                   ""uuid"": ""ba6494ce-7deb-492c-9799-408651e6bcaa""
                 },"
            });
            var secondNode = Diagram.Nodes.Add(new DiagramWidgetNode(position: new Point(200, 100))
            {
                Name = "Node 2",
                Content = @"{
    ""lorem_text"": ""repellat saepe veritatis"",""provident aut et"" ""provident aut et""
    ""ipv4"": ""96.0.190.30"",
    ""username"": ""Alberta.Kemmer86"",
    ""password""SitC2HQCQCwnxd7"",
    ""uuid"": ""d2853ce2-149b-4b20-ba9c-3a1ac9684365""
  },"
            });

            Diagram.Controls.AddFor(secondNode)
                .Add(new RemoveControl(1.085, -0.1));

            var leftPort = secondNode.AddPort(PortAlignment.Left);
            var rightPort = secondNode.AddPort(PortAlignment.Right);

            // The connection point will be the intersection of
            // a line going from the target to the center of the source
            var sourceAnchor = new ShapeIntersectionAnchor(firstNode);
            // The connection point will be the port's position
            var targetAnchor = new SinglePortAnchor(leftPort);
            var link = Diagram.Links.Add(new LinkModel(sourceAnchor, targetAnchor));
        }
    }
}
