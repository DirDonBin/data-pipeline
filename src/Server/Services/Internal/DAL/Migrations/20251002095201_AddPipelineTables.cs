using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Internal.DAL.Migrations
{
    /// <inheritdoc />
    public partial class AddPipelineTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropPrimaryKey(
                name: "PK_users",
                schema: "public",
                table: "users");

            migrationBuilder.RenameTable(
                name: "users",
                schema: "public",
                newName: "Users",
                newSchema: "public");

            migrationBuilder.RenameColumn(
                name: "email",
                schema: "public",
                table: "Users",
                newName: "Email");

            migrationBuilder.RenameColumn(
                name: "id",
                schema: "public",
                table: "Users",
                newName: "Id");

            migrationBuilder.RenameColumn(
                name: "password_salt",
                schema: "public",
                table: "Users",
                newName: "PasswordSalt");

            migrationBuilder.RenameColumn(
                name: "password_hash",
                schema: "public",
                table: "Users",
                newName: "PasswordHash");

            migrationBuilder.RenameColumn(
                name: "last_name",
                schema: "public",
                table: "Users",
                newName: "LastName");

            migrationBuilder.RenameColumn(
                name: "first_name",
                schema: "public",
                table: "Users",
                newName: "FirstName");

            migrationBuilder.RenameColumn(
                name: "created_at",
                schema: "public",
                table: "Users",
                newName: "CreatedAt");

            migrationBuilder.RenameIndex(
                name: "IX_users_email",
                schema: "public",
                table: "Users",
                newName: "IX_Users_Email");

            migrationBuilder.AddPrimaryKey(
                name: "PK_Users",
                schema: "public",
                table: "Users",
                column: "Id");

            migrationBuilder.CreateTable(
                name: "Pipelines",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    Description = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true),
                    CreatedUserId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    IsActive = table.Column<bool>(type: "boolean", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Pipelines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Pipelines_Users_CreatedUserId",
                        column: x => x.CreatedUserId,
                        principalSchema: "public",
                        principalTable: "Users",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PipelineDataNodes",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PipelineId = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    NodeType = table.Column<string>(type: "text", nullable: false),
                    DataNodeType = table.Column<string>(type: "text", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PipelineDataNodes", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PipelineDataNodes_Pipelines_PipelineId",
                        column: x => x.PipelineId,
                        principalSchema: "public",
                        principalTable: "Pipelines",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PipelineFunctions",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PipelineId = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    Function = table.Column<string>(type: "text", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PipelineFunctions", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PipelineFunctions_Pipelines_PipelineId",
                        column: x => x.PipelineId,
                        principalSchema: "public",
                        principalTable: "Pipelines",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PipelineRelationships",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PipelineId = table.Column<Guid>(type: "uuid", nullable: false),
                    FromId = table.Column<Guid>(type: "uuid", nullable: false),
                    ToId = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderIndex = table.Column<int>(type: "integer", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PipelineRelationships", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PipelineRelationships_Pipelines_PipelineId",
                        column: x => x.PipelineId,
                        principalSchema: "public",
                        principalTable: "Pipelines",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PipelineRuns",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PipelineId = table.Column<Guid>(type: "uuid", nullable: false),
                    StartedByUserId = table.Column<Guid>(type: "uuid", nullable: false),
                    Status = table.Column<int>(type: "integer", nullable: false),
                    StartedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CompletedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    DurationSeconds = table.Column<int>(type: "integer", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PipelineRuns", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PipelineRuns_Pipelines_PipelineId",
                        column: x => x.PipelineId,
                        principalSchema: "public",
                        principalTable: "Pipelines",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_PipelineRuns_Users_StartedByUserId",
                        column: x => x.StartedByUserId,
                        principalSchema: "public",
                        principalTable: "Users",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "DatabaseNodeConfigs",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    DataNodeId = table.Column<Guid>(type: "uuid", nullable: false),
                    DatabaseType = table.Column<byte>(type: "smallint", nullable: false),
                    Host = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    Port = table.Column<int>(type: "integer", nullable: false),
                    Database = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    Username = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    Password = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    AdditionalParams = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DatabaseNodeConfigs", x => x.Id);
                    table.ForeignKey(
                        name: "FK_DatabaseNodeConfigs_PipelineDataNodes_DataNodeId",
                        column: x => x.DataNodeId,
                        principalSchema: "public",
                        principalTable: "PipelineDataNodes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FileNodeConfigs",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    DataNodeId = table.Column<Guid>(type: "uuid", nullable: false),
                    FileUrl = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: false),
                    FileType = table.Column<byte>(type: "smallint", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FileNodeConfigs", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FileNodeConfigs_PipelineDataNodes_DataNodeId",
                        column: x => x.DataNodeId,
                        principalSchema: "public",
                        principalTable: "PipelineDataNodes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PipelineRunDetails",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PipelineRunId = table.Column<Guid>(type: "uuid", nullable: false),
                    ErrorMessage = table.Column<string>(type: "character varying(5000)", maxLength: 5000, nullable: true),
                    ResultJson = table.Column<string>(type: "text", nullable: true),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PipelineRunDetails", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PipelineRunDetails_PipelineRuns_PipelineRunId",
                        column: x => x.PipelineRunId,
                        principalSchema: "public",
                        principalTable: "PipelineRuns",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_DatabaseNodeConfigs_DataNodeId",
                schema: "public",
                table: "DatabaseNodeConfigs",
                column: "DataNodeId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_FileNodeConfigs_DataNodeId",
                schema: "public",
                table: "FileNodeConfigs",
                column: "DataNodeId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_PipelineDataNodes_PipelineId",
                schema: "public",
                table: "PipelineDataNodes",
                column: "PipelineId");

            migrationBuilder.CreateIndex(
                name: "IX_PipelineDataNodes_PipelineId_NodeType",
                schema: "public",
                table: "PipelineDataNodes",
                columns: new[] { "PipelineId", "NodeType" });

            migrationBuilder.CreateIndex(
                name: "IX_PipelineFunctions_PipelineId",
                schema: "public",
                table: "PipelineFunctions",
                column: "PipelineId");

            migrationBuilder.CreateIndex(
                name: "IX_PipelineRelationships_PipelineId",
                schema: "public",
                table: "PipelineRelationships",
                column: "PipelineId");

            migrationBuilder.CreateIndex(
                name: "IX_PipelineRunDetails_PipelineRunId",
                schema: "public",
                table: "PipelineRunDetails",
                column: "PipelineRunId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_PipelineRuns_PipelineId",
                schema: "public",
                table: "PipelineRuns",
                column: "PipelineId");

            migrationBuilder.CreateIndex(
                name: "IX_PipelineRuns_StartedByUserId",
                schema: "public",
                table: "PipelineRuns",
                column: "StartedByUserId");

            migrationBuilder.CreateIndex(
                name: "IX_Pipelines_CreatedAt",
                schema: "public",
                table: "Pipelines",
                column: "CreatedAt");

            migrationBuilder.CreateIndex(
                name: "IX_Pipelines_CreatedUserId",
                schema: "public",
                table: "Pipelines",
                column: "CreatedUserId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "DatabaseNodeConfigs",
                schema: "public");

            migrationBuilder.DropTable(
                name: "FileNodeConfigs",
                schema: "public");

            migrationBuilder.DropTable(
                name: "PipelineFunctions",
                schema: "public");

            migrationBuilder.DropTable(
                name: "PipelineRelationships",
                schema: "public");

            migrationBuilder.DropTable(
                name: "PipelineRunDetails",
                schema: "public");

            migrationBuilder.DropTable(
                name: "PipelineDataNodes",
                schema: "public");

            migrationBuilder.DropTable(
                name: "PipelineRuns",
                schema: "public");

            migrationBuilder.DropTable(
                name: "Pipelines",
                schema: "public");

            migrationBuilder.DropPrimaryKey(
                name: "PK_Users",
                schema: "public",
                table: "Users");

            migrationBuilder.RenameTable(
                name: "Users",
                schema: "public",
                newName: "users",
                newSchema: "public");

            migrationBuilder.RenameColumn(
                name: "Email",
                schema: "public",
                table: "users",
                newName: "email");

            migrationBuilder.RenameColumn(
                name: "Id",
                schema: "public",
                table: "users",
                newName: "id");

            migrationBuilder.RenameColumn(
                name: "PasswordSalt",
                schema: "public",
                table: "users",
                newName: "password_salt");

            migrationBuilder.RenameColumn(
                name: "PasswordHash",
                schema: "public",
                table: "users",
                newName: "password_hash");

            migrationBuilder.RenameColumn(
                name: "LastName",
                schema: "public",
                table: "users",
                newName: "last_name");

            migrationBuilder.RenameColumn(
                name: "FirstName",
                schema: "public",
                table: "users",
                newName: "first_name");

            migrationBuilder.RenameColumn(
                name: "CreatedAt",
                schema: "public",
                table: "users",
                newName: "created_at");

            migrationBuilder.RenameIndex(
                name: "IX_Users_Email",
                schema: "public",
                table: "users",
                newName: "IX_users_email");

            migrationBuilder.AddPrimaryKey(
                name: "PK_users",
                schema: "public",
                table: "users",
                column: "id");
        }
    }
}
