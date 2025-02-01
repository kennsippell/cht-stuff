import _ from 'lodash';

import { Config } from 'cht-user-management/src/config';
import { ChtApi } from 'cht-user-management/src/lib/cht-api';
import ChtSession from 'cht-user-management/src/lib/cht-session';
import RemotePlaceCache from 'cht-user-management/src/lib/remote-place-cache';

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
  
  const chpType = Config.getContactType('d_community_health_volunteer_area');
  const subcounties = await RemotePlaceCache.getRemotePlaces(chtApi, chpType, chpType.hierarchy[1]);
  for (const subcounty of subcounties) {
    console.log(subcounty.id, subcounty.name.formatted)
  }
})();