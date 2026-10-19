const fs = require('fs');
const { queryParams } = require('./utils');
const marked = require('marked').marked;
const PROJECTS_PATH = __dirname + '/../projects';
const fetch = require('node-fetch').default;

const projects_including_external = {}, projects = {};
fs.readdirSync(PROJECTS_PATH).forEach(projectDir => {
	try {
		const project = JSON.parse(fs.readFileSync(PROJECTS_PATH + '/' + projectDir + '/info.json'));
		project.howto = marked(fs.readFileSync(PROJECTS_PATH + '/' + projectDir + '/howto.md', "utf8"));
		project.month = project.name.split("_").shift() + "-05T00:00:00Z";
		project.slug = project.name.split("_").pop();
		project.icon = `/images/badges/${projectDir.split("_").pop()}.svg`;

		// Check dates consistency
		[["start_date", "end_date"],
		["start_date", "soft_start_date"],
		["soft_start_date", "soft_end_date"],
		["soft_end_date", "end_date"]].forEach(([startKey, endKey]) => {
			const start = project[startKey], end = project[endKey];
			if (start != null && end != null && new Date(end).getTime() < new Date(start).getTime()) {
				throw new Error(`${endKey} (${end}) is before ${startKey} (${start})`);
			}
		});

		if (project.statistics && project.database?.osmium_tag_filter) {
			// Filtered features
			let tagFilterParts = project.database.osmium_tag_filter.split("&");
			project.database.tagFilterFeatures = "nwr";
			tagFilterParts.forEach(tagFilter => {
				if (tagFilter.indexOf('/') > -1) {
					project.database.tagFilterFeatures = (project.database.tagFilterFeatures.match(new RegExp('[' + tagFilter.split('/').shift() + ']', 'g')) || []).join('');
				}
			});

			// Add auto-computed metadata
			project.osmoseLabels = {};
			project.osmoseButtons = {};
			project.datasources
				.filter(ds => ds.source === "osmose")
				.forEach(ds => {
					project.osmoseLabels[`${ds.item}_${ds.class || "all"}`] = {
						name: ds.name,
						subtitles: ds.subtitles,
						description: ds.description
					};
					project.osmoseButtons[`${ds.item}_${ds.class || "all"}`] = ds.buttons;
				});

			if (project.editors){
				project.idParams = queryParams(Object.assign({}, project.editors.all, project.editors.iD));
				project.josmParams = queryParams(Object.assign({ changeset_comment: project.editors.all.comment, changeset_hashtags: project.editors.all.hashtags.split(",").join(" #") }, project.editors.JOSM));

				// Replace NSI editors fields by actual value
				if(project.editors.pdm && project.editors.pdm.fields) {
					project.editors.pdm.fields.map(async (f, id) => {
						if (f.type === "nsi") {
							const nsi = await (await fetch(`https://github.com/osmlab/name-suggestion-index/raw/main/data/${f.path}.json`)).json();
							f.type = "select";
							f.tag = "_select" + id;

							f.values = nsi.items
								.filter(it => !f.locationSet || !it.locationSet || !it.locationSet.include || it.locationSet.include.includes("001") || it.locationSet.include.includes(f.locationSet))
								.map(it => ({
									l: it.displayName,
									tags: it.tags
								}));

							delete f.path;
							delete f.locationSet;
						}

						return f;
					});
				}
			}
			projects[project.name] = project;
		} else if (!project.links?.external_statistics) {
			throw new Error("Missing both database.osmium_tag_filter and links.external_statistics in info.json");
		}

		projects_including_external[project.name] = project;
	}
	catch (e) {
		console.error("Invalid project", projectDir, e);
	}
});

// Projects init list
const projects_init_list = Array(Object.keys(projects).length);
let lastSlot = -1;
Object.values(projects).forEach (project => {
	if (project.hasOwnProperty("init_slot")){
		if (projects_init_list[project.init_slot-1] != null){
			projects_init_list.splice(project.init_slot-1, 0, project.name);
		}else{
			projects_init_list[project.init_slot-1] = project.name;
		}
		lastSlot = Math.max(lastSlot, project.init_slot-1);
	}else{
		projects_init_list[lastSlot+1] = project.name;
	}
});

module.exports = { projects_including_external, projects, projects_init_list };
