const {
  getMapStatsStyle
} = require("./utils");

let promise_lastUpdate = (pool, project_id) => {
  return pool
  .query(`SELECT changes_lastupdate_date, counts_lastupdate_date FROM pdm_projects WHERE project_id = $1`, [
    project_id,
  ])
  .then((results) => ({
    lastUpdate: results.rows?.length > 0 && results.rows[0].changes_lastupdate_date,
    lastCount: results.rows?.length > 0 && results.rows[0].counts_lastupdate_date,
  }));
};

let promises_featureCounts = (pool, project, boundary) => {
  const allPromises = [];

  if (project.statistics.count) {
    let qryParams = [project.id];
    let table = "pdm_feature_counts"
    let boundaryWhere = "";
    if (boundary != null){
      table = "pdm_feature_counts_per_boundary"
      boundaryWhere = " AND boundary = $2"
      qryParams = [project.id, boundary]
    }

    allPromises.push(
      pool
        .query(
          `
      SELECT ts, label, amount, len, area
      FROM ${table}
      WHERE project_id = $1 ${boundaryWhere}
      ORDER BY ts ASC
    `,
          qryParams
        )
        .then((results) => {
          const records = results.rows.reduce((acc, row) => {
            if (!acc[row.ts]) {
              acc[row.ts] = { x: row.ts, labels: {} };
            }
            if (row.label == null) {
              acc[row.ts].amount = row.amount;
              acc[row.ts].length = row.len;
              acc[row.ts].area = row.area;
            } else {
              acc[row.ts].labels[row.label] = { amount: row.amount, length: row.len, area: row.area };
            }
            return acc;
          }, {});

          return { data: Object.values(records) };
        }),
    );
  }

  // Fetch last count update time
  allPromises.push(promise_lastUpdate(pool, project.id));

  return allPromises;
}

let promises_featureContribs = (pool, project, team, user) => {
  let allPromises = [];

  if (project.statistics.count) {
    let qryParams = [project.id];
    let filterWhere = "uc.project_id = $1";
    let teamJoin = "";
    if (team != null){
      teamJoin = "JOIN pdm_projects_teams pt ON pt.project_id=uc.project_id AND pt.userid=uc.userid";
      filterWhere += " AND pt.team = $2"
      qryParams = [project.id, team]
    }else if (user != null){
      filterWhere += " AND uc.userid = $2"
      qryParams = [project.id, user]
    }

    allPromises.push(
      pool
        .query(
          `
          SELECT uc.ts, uc.label, SUM(uc.amount_delta) as amount_delta, SUM(uc.len_delta) as len_delta, SUM(uc.area_delta) as area_delta, SUM(uc.points) as points
          FROM pdm_user_contribs uc
          ${teamJoin}
          WHERE ${filterWhere}
          GROUP BY uc.ts, uc.label
          ORDER BY uc.ts, uc.label ASC
        `,
          qryParams
        )
        .then((results) => {
          const records = results.rows.reduce((acc, row) => {
            if (!acc[row.ts]) {
              acc[row.ts] = { x: row.ts, labels: {} };
            }
            if (row.label == null) {
              acc[row.ts].amount_delta = row.amount_delta;
              acc[row.ts].length_delta = row.len_delta;
              acc[row.ts].area_delta = row.area_delta;
              acc[row.ts].points = row.points;
            } else {
              acc[row.ts].labels[row.label] = { amount_delta: row.amount_delta, length_delta: row.len_delta, area_delta: row.area_delta, points: row.points };
            }
            return acc;
          }, {});

          return { data: Object.values(records) };
        }),
    );
  }

  // Fetch last count update time
  allPromises.push(promise_lastUpdate(pool, project.id));

  return allPromises;
}

let promises_mappersCounts = (pool, project, boundary) => {
  let allPromises = [];

  if (project.statistics.count) {
    let qryParams = [project.id];
    let table = "pdm_mapper_counts"
    let boundaryWhere = "";
    if (boundary != null){
      table = "pdm_mapper_counts_per_boundary"
      boundaryWhere = " AND boundary = $2"
      qryParams = [project.id, boundary]
    }

    allPromises.push(
      pool
        .query(
          `
      SELECT ts, label, amount, amount_1d, amount_30d
      FROM ${table}
      WHERE project_id = $1 ${boundaryWhere}
      ORDER BY ts ASC
    `,
          qryParams
        )
        .then((results) => {
          const records = results.rows.reduce((acc, row) => {
            if (!acc[row.ts]) {
              acc[row.ts] = { x: row.ts, labels: {} };
            }
            if (row.label == null) {
              acc[row.ts].amount = row.amount;
              acc[row.ts].amount_1d = row.amount_1d;
              acc[row.ts].amount_30d = row.amount_30d;
            } else {
              acc[row.ts].labels[row.label] = { amount: row.amount, amount_1d: row.amount_1d, amount_30d: row.amount_30d };
            }
            return acc;
          }, {});

          return { data: Object.values(records) };
        })
    );
  }

  // Fetch last count update time
  allPromises.push(promise_lastUpdate(pool, project.id));

  return allPromises;
}

let promises_mappersTeamCounts = (pool, project, team) => {
  const allPromises = [];
  if (project.statistics.count) {
    allPromises.push(
      pool
        .query(
          `
          SELECT uc.ts, uc.label, count(distinct uc.userid) as mappers_period, count(distinct uc.userid) over (partition by project_id, label order by ts) as mappers
          FROM pdm_user_contribs uc
          JOIN pdm_projects_teams pt ON pt.project_id=uc.project_id AND pt.userid=uc.userid
          WHERE uc.project_id = $1 AND pt.team = $2
          GROUP BY uc.ts, uc.label
          ORDER BY uc.ts, uc.label ASC
          `,
          [project.id, team]
        )
        .then((results) => {
          const records = results.rows.reduce((acc, row) => {
            if (!acc[row.ts]) {
              acc[row.ts] = { x: row.ts, labels: {} };
            }
            if (row.label == null) {
              acc[row.ts].amount = row.mappers;
              acc[row.ts].amount_period = row.mappers_period;
            } else {
              acc[row.ts].labels[row.label] = { amount: row.mappers, amount_period: row.mappers_period };
            }
            return acc;
          }, {});

          return { data: Object.values(records) };
        })
    );
  }

  // Fetch last count update time
  allPromises.push(promise_lastUpdate(project.id));

  return allPromises;
}

let promises_projectStats = (pool, project) => {
  const allPromises = [];

  // Fetch feature counts
  if (project.statistics.count) {
    allPromises.push(
    pool.query(
      `
      SELECT ts, amount
      FROM pdm_feature_counts
      WHERE project_id = $1 AND label IS NULL
      ORDER BY ts ASC
      `,
      [project.id]
    )
    .then((results) => ({
      chart: [
        {
        label: "Count in OSM",
        data: results.rows.map((r) => ({ x: r.ts, y: r.amount })),
        fill: false,
        borderColor: "#388E3C",
        lineTension: 0,
        },
      ],
      "daily":{
        "ts": results.rows.length > 0 &&
        results.rows[results.rows.length - 1].ts,
        "ts_start": results.rows[0].ts,
        "count":results.rows.length > 0 &&
        results.rows[results.rows.length - 1].amount,
        "added":
        results.rows.length > 0 &&
        results.rows[results.rows.length - 1].amount -
          results.rows[0].amount
      },
      "past": {
        "ts": results.rows.length > 1 &&
        results.rows[results.rows.length - 2].ts,
        "ts_start": results.rows[0].ts,
        "count": results.rows.length > 1 &&
        results.rows[results.rows.length - 2].amount,
        "added":
        results.rows.length > 1 &&
        results.rows[results.rows.length - 2].amount -
          results.rows[0].amount
      }
      })),
    );

    // Current time point is only available if a live table is maintained
    if (project.database.live){
      allPromises.push(
        pool.query(
          `SELECT COUNT(*) AS amount FROM pdm_project_${project.name.split("_").pop()}`,
        )
        .then((results) => ({
          "current":{
            "count": results.rows.length > 0 && parseInt(results.rows[0].amount)
          }
        })),
      );
    }

    if (project.datasources.find((ds) => ds.source === "stats")) {
      allPromises.push(
        pool.query(
          `SELECT d.admin_level, d.delta_project_min, d.delta_project_max, d.delta_daily_min, d.delta_daily_max, b.name as boundary_max FROM pdm_boundary_dash d JOIN pdm_boundary b ON b.osm_id=d.boundary_max WHERE project_id = $1 AND label IS NULL`,[
          project.id
        ])
        .then((results) => {
          const prjDeltaLevel = {};
          const dailyDeltaLevel = {};
          results.rows.forEach((r) => {
            if (!isNaN(parseInt(r.delta_project_max))) {
              prjDeltaLevel[r.admin_level] = {"min":r.delta_project_min, "max":r.delta_project_max, "boundary": r.boundary_max};
            }
            dailyDeltaLevel[r.admin_level] = [r.delta_daily_min, r.delta_daily_max];
          });
          return Object.keys(prjDeltaLevel).length > 0
          ? getMapStatsStyle(project, prjDeltaLevel, dailyDeltaLevel)
          : null;
        })
        .then((mapStyle) => ({ mapStyle })),
      );
    }
  }

  // Fetch mappers count
  allPromises.push(
    pool.query(
	  `SELECT * FROM pdm_mapper_counts WHERE project_id = $1 and label is null ORDER BY ts DESC limit 2`, [
      project.id,
    ])
    .then((results) => ({
      "daily":{
        nbContributors: results.rows[0].amount,
        nbContributors_1d: results.rows[0].amount_1d,
        nbContributors_30d: results.rows[0].amount_30d
      },
      "past": {
        nbContributors: results.rows.length > 1 && results.rows[1].amount,
        nbContributors_1d: results.rows.length > 1 && results.rows[1].amount_1d,
        nbContributors_30d: results.rows.length > 1 && results.rows[1].amount_30d
      }
    })),
  );

  allPromises.push(promise_lastUpdate(pool, project.id));

  return allPromises;
}

let promises_projectsSummary = (pool) => {
  const allPromises = [];

  allPromises.push(
    pool.query(
      `
      SELECT distinct
      fc.project_id,
      fc.label,
      first_value(fc.ts) over project as ts,
      nth_value(fc.ts, 2) over project as ts_prev,
      first_value(fc.amount) over project as features,
      nth_value(fc.amount, 2) over project as features_prev,
      first_value(fc.amount) over project - nth_value(fc.amount, 2) over project as features_delta,
      first_value(mc.amount) over project as mappers,
      nth_value(mc.amount, 2) over project as mappers_prev,
      first_value(mc.amount) over project - nth_value(mc.amount, 2) over project as mappers_delta
      FROM pdm_feature_counts fc
      JOIN pdm_mapper_counts mc ON mc.project_id=fc.project_id AND mc.ts=fc.ts and coalesce(mc.label,'_global')=coalesce(fc.label,'_global')
      WHERE fc.label is null
      WINDOW project as (PARTITION BY mc.project_id, mc.label order by mc.ts desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      `
    )
    .then((results) => {
      return results.rows.reduce((acc, r) => {
        acc.push({
          project_id: r.project_id,
          ts: r.ts,
          features: r.features,
          features_prev: r.features_prev,
          features_delta: r.features_delta,
          mappers: r.mappers,
          mappers_prev: r.mappers_prev,
          mappers_delta: r.mappers_delta,
        });
        return acc;
      }, []);
    }),
  );

  return allPromises;
}

module.exports = {promise_lastUpdate, promises_featureCounts, promises_featureContribs, promises_mappersCounts, promises_mappersTeamCounts, promises_projectStats, promises_projectsSummary}