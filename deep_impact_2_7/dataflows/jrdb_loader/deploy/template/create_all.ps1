function change_file_enc ($src, $dest) {
    Get-Content -Path $src -Raw | % { [Text.Encoding]::UTF8.GetBytes($_) } | Set-Content -Path $dest -Encoding Byte
    del $src
}



@("bac","kab","kza","kyi","sed","ukc") | %{
    $table = $_
    content "#deploy_template.cmd" | %{
        $_ -replace "@template@",$table | add-content -encoding string -path ("../deploy_" + $table + ".cmd_r")
    }
    change_file_enc ("../deploy_" + $table + ".cmd_r") ("../deploy_" + $table + ".cmd")
    content "#deploy_template_metadata.cmd" | %{
        $_ -replace "@template@",$table | add-content -encoding string -path ("../deploy_" + $table + "_metadata.cmd_r")
    }
    change_file_enc ("../deploy_" + $table + "_metadata.cmd_r") ("../deploy_" + $table + "_metadata.cmd")
    content "#jrdb_template_loader_metadata" | %{
        $_ -replace "@template@",$table | add-content -encoding string -path ("../jrdb_" + $table + "_loader_metadata_r")
    }
    change_file_enc ("../jrdb_" + $table + "_loader_metadata_r") ("../jrdb_" + $table + "_loader_metadata")

}