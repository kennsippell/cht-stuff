import _ from 'lodash';

import { Config } from 'cht-user-management/src/config';
import { ChtApi } from 'cht-user-management/src/lib/cht-api';
import ChtSession from 'cht-user-management/src/lib/cht-session';
import RemotePlaceCache from 'cht-user-management/src/lib/remote-place-cache';

// curl "http://medic:password@localhost:5988/medic/_all_docs?include_docs=true&startkey=%22target~2025-01~%22&endkey=%22target~2025-01~z%22" > target-docs.json
import * as JanuaryTargets from '../target-docs.json';
import { DateTime } from 'luxon';

const authInfo = {
  friendly: 'Local Dev',
  domain: 'localhost:5988',
  useHttp: true,
};
const username = 'medic';
const password = 'password';

(async function() {
  const session = await ChtSession.create(authInfo, username, password);
  const chtApi = new ChtApi(session);
  
  const results: { [key: string]: any } = {};
  for (const row of JanuaryTargets.rows) {
    const referrals = row.doc.targets.find(t => t.id === 'total-referrals');
    const visits = row.doc.targets.find(t => t.id === 'count-households-visited');
    const updated = DateTime.fromMillis(row.doc.updated_date);
    results[row.id] = {
      user: row.doc.user,
      contactId: row.doc.owner,
      referrals: referrals?.value.total,
      visits: visits?.value.pass,
      updated: updated.toISODate(),
    };
  }

  const chpType = Config.getContactType('d_community_health_volunteer_area');
  const chpAreas = await RemotePlaceCache.getRemotePlaces(chtApi, chpType, chpType.hierarchy[2]);
  for (const result of Object.values(results)) {
    const match = chpAreas.find(area => area.contactId === result.contactId);
    if (match) {
      result.chp_area = match.name.formatted;
      result.subcounty_id = match.lineage[1];
      result.chu_id = match.lineage[0];
    }
  }

  console.table(Object.values(results));
})();